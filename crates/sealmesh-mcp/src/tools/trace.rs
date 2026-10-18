//! Every name a tool's `Args` type actually accepts, read from the type.
//!
//! The seam checks the schema a tool *shows*. What matters is what its
//! handler *parses*, and the two can disagree: a variant skipped in a `schemars` attribute
//! the schema hides, a `serde(alias)` it never mentions — including one
//! behind a `cfg_attr` or emitted by a macro, where no text scan finds it.
//! So this asks serde itself.
//!
//! [`accepted`] drives `T::deserialize` with a [`Tracer`], a deserializer
//! that answers every request with a plausible value and records the names
//! serde's derive hands it: the `fields` of each struct (aliases included,
//! since the derive lists them there) and the `variants` of each enum. It
//! re-runs once per variant not yet visited, so a type nested inside any
//! variant is reached. Anything it cannot trace — `deserialize_any`, a map,
//! an identifier outside a struct or an enum — is recorded as untraceable,
//! which the probe refuses: a shape that cannot be enumerated cannot be
//! shown to match its schema.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde::de::{
    self, DeserializeOwned, DeserializeSeed, Deserializer, EnumAccess, IntoDeserializer, MapAccess,
    SeqAccess, VariantAccess, Visitor,
};

/// How deep a traced type may nest before it is refused rather than
/// followed: a recursive type would otherwise never end.
const MAX_DEPTH: usize = 32;

/// How many runs one type may take, one per unvisited variant.
const MAX_RUNS: usize = 256;

/// What a type accepts.
#[derive(Debug, Default)]
pub(crate) struct Accepted {
    pub(crate) fields: BTreeSet<String>,
    pub(crate) variants: BTreeSet<String>,
    pub(crate) untraceable: BTreeSet<String>,
}

#[derive(Default)]
struct State {
    accepted: Accepted,
    /// Which variant each enum answers with on this run.
    choices: BTreeMap<&'static str, usize>,
    /// Each enum seen, with how many variants it has.
    enums: BTreeMap<&'static str, usize>,
}

#[derive(Debug)]
pub(crate) struct TraceError(String);

impl fmt::Display for TraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TraceError {}

impl de::Error for TraceError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

/// Every field and variant name `T` accepts, at every level.
pub(crate) fn accepted<T: DeserializeOwned>() -> Result<Accepted, String> {
    let state = RefCell::new(State::default());
    let mut visited: BTreeMap<&'static str, BTreeSet<usize>> = BTreeMap::new();
    for _ in 0..MAX_RUNS {
        let run = T::deserialize(Tracer {
            state: &state,
            depth: 0,
        });
        let mut current = state.borrow_mut();
        if let Err(error) = run {
            current.accepted.untraceable.insert(error.0);
        }
        let State {
            accepted: _,
            choices,
            enums,
        } = &mut *current;
        for name in enums.keys() {
            visited
                .entry(name)
                .or_default()
                .insert(choices.get(name).copied().unwrap_or(0));
        }
        let next = enums.iter().find_map(|(name, len)| {
            (0..*len)
                .find(|index| !visited.get(name).is_some_and(|seen| seen.contains(index)))
                .map(|index| (*name, index))
        });
        match next {
            Some((name, index)) => {
                choices.insert(name, index);
            }
            None => return Ok(std::mem::take(&mut current.accepted)),
        }
    }
    Err(format!("more than {MAX_RUNS} variants to visit"))
}

struct Tracer<'s> {
    state: &'s RefCell<State>,
    depth: usize,
}

impl<'s> Tracer<'s> {
    fn deeper(&self) -> Result<Self, TraceError> {
        if self.depth >= MAX_DEPTH {
            return Err(self.untraceable("nesting past the depth cap"));
        }
        Ok(Self {
            state: self.state,
            depth: self.depth.saturating_add(1),
        })
    }

    fn untraceable(&self, what: &str) -> TraceError {
        let message = format!("untraceable: {what}");
        self.state
            .borrow_mut()
            .accepted
            .untraceable
            .insert(message.clone());
        TraceError(message)
    }

    fn record_fields(&self, fields: &'static [&'static str]) {
        let mut state = self.state.borrow_mut();
        for field in fields {
            state.accepted.fields.insert((*field).to_owned());
        }
    }
}

macro_rules! primitive {
    ($method:ident, $visit:ident, $value:expr) => {
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
            visitor.$visit($value)
        }
    };
}

impl<'de, 's> Deserializer<'de> for Tracer<'s> {
    type Error = TraceError;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, TraceError> {
        Err(self.untraceable("a self-describing value (deserialize_any)"))
    }

    primitive!(deserialize_bool, visit_bool, false);
    primitive!(deserialize_i8, visit_i8, 1);
    primitive!(deserialize_i16, visit_i16, 1);
    primitive!(deserialize_i32, visit_i32, 1);
    primitive!(deserialize_i64, visit_i64, 1);
    primitive!(deserialize_i128, visit_i128, 1);
    primitive!(deserialize_u8, visit_u8, 1);
    primitive!(deserialize_u16, visit_u16, 1);
    primitive!(deserialize_u32, visit_u32, 1);
    primitive!(deserialize_u64, visit_u64, 1);
    primitive!(deserialize_u128, visit_u128, 1);
    primitive!(deserialize_f32, visit_f32, 1.0);
    primitive!(deserialize_f64, visit_f64, 1.0);
    primitive!(deserialize_char, visit_char, 'x');
    primitive!(deserialize_str, visit_str, "x");
    primitive!(deserialize_string, visit_str, "x");
    primitive!(deserialize_bytes, visit_bytes, b"x");
    primitive!(deserialize_byte_buf, visit_bytes, b"x");

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        visitor.visit_some(self.deeper()?)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        visitor.visit_newtype_struct(self.deeper()?)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        visitor.visit_seq(Elements {
            tracer: self.deeper()?,
            left: 1,
        })
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        visitor.visit_seq(Elements {
            tracer: self.deeper()?,
            left: len,
        })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, TraceError> {
        // A map admits keys nobody listed; `flatten` also lands here.
        Err(self.untraceable("a map (a map type, or a struct with a flattened field)"))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        self.record_fields(fields);
        visitor.visit_map(Fields {
            tracer: self.deeper()?,
            fields: fields.iter(),
        })
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        let chosen = {
            let mut state = self.state.borrow_mut();
            for variant in variants {
                state.accepted.variants.insert((*variant).to_owned());
            }
            state.enums.insert(name, variants.len());
            state.choices.get(name).copied().unwrap_or(0)
        };
        let Some(variant) = variants.get(chosen) else {
            return Err(self.untraceable("an enum with no variants"));
        };
        visitor.visit_enum(Choice {
            tracer: self.deeper()?,
            variant,
        })
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, TraceError> {
        Err(self.untraceable("an identifier outside a struct or an enum"))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TraceError> {
        visitor.visit_unit()
    }
}

struct Elements<'s> {
    tracer: Tracer<'s>,
    left: usize,
}

impl<'de, 's> SeqAccess<'de> for Elements<'s> {
    type Error = TraceError;

    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, TraceError> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left = self.left.saturating_sub(1);
        seed.deserialize(Tracer {
            state: self.tracer.state,
            depth: self.tracer.depth,
        })
        .map(Some)
    }
}

struct Fields<'s> {
    tracer: Tracer<'s>,
    fields: std::slice::Iter<'static, &'static str>,
}

impl<'de, 's> MapAccess<'de> for Fields<'s> {
    type Error = TraceError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, TraceError> {
        match self.fields.next() {
            Some(field) => seed
                .deserialize(IntoDeserializer::<TraceError>::into_deserializer(*field))
                .map(Some),
            None => Ok(None),
        }
    }

    fn next_value_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<S::Value, TraceError> {
        seed.deserialize(Tracer {
            state: self.tracer.state,
            depth: self.tracer.depth,
        })
    }
}

struct Choice<'s> {
    tracer: Tracer<'s>,
    variant: &'static str,
}

impl<'de, 's> EnumAccess<'de> for Choice<'s> {
    type Error = TraceError;
    type Variant = Tracer<'s>;

    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, Tracer<'s>), TraceError> {
        let value = seed.deserialize(IntoDeserializer::<TraceError>::into_deserializer(
            self.variant,
        ))?;
        Ok((value, self.tracer))
    }
}

impl<'de, 's> VariantAccess<'de> for Tracer<'s> {
    type Error = TraceError;

    fn unit_variant(self) -> Result<(), TraceError> {
        Ok(())
    }

    fn newtype_variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<S::Value, TraceError> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        Deserializer::deserialize_tuple(self, len, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, TraceError> {
        Deserializer::deserialize_struct(self, "", fields, visitor)
    }
}
