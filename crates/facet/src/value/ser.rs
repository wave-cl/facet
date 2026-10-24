//! `Serialize` → `Value`.

use std::collections::BTreeMap;

use serde::ser::{self, Serialize};

use super::Value;
use crate::error::Error;

pub(super) struct ValueSerializer;

pub(super) struct SeqSer {
    items: Vec<Value>,
    variant: Option<&'static str>,
}

pub(super) struct MapSer {
    map: BTreeMap<String, Value>,
    next_key: Option<String>,
    variant: Option<&'static str>,
}

fn key_string(v: Value) -> Result<String, Error> {
    Ok(match v {
        Value::Str(s) => s,
        Value::Int(i) => i.to_string(),
        Value::Uint(u) => u.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Uuid(u) => u.hyphenated().to_string(),
        Value::Decimal(d) => d.to_string(),
        other => {
            return Err(Error::Value(format!(
                "a map key must be a string or number, not {}",
                other.type_name()
            )));
        }
    })
}

fn wrap(variant: Option<&'static str>, v: Value) -> Value {
    match variant {
        None => v,
        Some(name) => {
            let mut m = BTreeMap::new();
            m.insert(name.to_owned(), v);
            Value::Map(m)
        }
    }
}

impl ser::Serializer for ValueSerializer {
    type Ok = Value;
    type Error = Error;
    type SerializeSeq = SeqSer;
    type SerializeTuple = SeqSer;
    type SerializeTupleStruct = SeqSer;
    type SerializeTupleVariant = SeqSer;
    type SerializeMap = MapSer;
    type SerializeStruct = MapSer;
    type SerializeStructVariant = MapSer;

    fn serialize_bool(self, v: bool) -> Result<Value, Error> {
        Ok(Value::Bool(v))
    }
    fn serialize_i8(self, v: i8) -> Result<Value, Error> {
        Ok(Value::Int(v.into()))
    }
    fn serialize_i16(self, v: i16) -> Result<Value, Error> {
        Ok(Value::Int(v.into()))
    }
    fn serialize_i32(self, v: i32) -> Result<Value, Error> {
        Ok(Value::Int(v.into()))
    }
    fn serialize_i64(self, v: i64) -> Result<Value, Error> {
        Ok(Value::Int(v))
    }
    fn serialize_i128(self, v: i128) -> Result<Value, Error> {
        i64::try_from(v)
            .map(Value::Int)
            .map_err(|_| Error::Value(format!("{v} does not fit an int")))
    }
    fn serialize_u8(self, v: u8) -> Result<Value, Error> {
        Ok(Value::Uint(v.into()))
    }
    fn serialize_u16(self, v: u16) -> Result<Value, Error> {
        Ok(Value::Uint(v.into()))
    }
    fn serialize_u32(self, v: u32) -> Result<Value, Error> {
        Ok(Value::Uint(v.into()))
    }
    fn serialize_u64(self, v: u64) -> Result<Value, Error> {
        Ok(Value::Uint(v))
    }
    fn serialize_u128(self, v: u128) -> Result<Value, Error> {
        u64::try_from(v)
            .map(Value::Uint)
            .map_err(|_| Error::Value(format!("{v} does not fit a uint")))
    }
    fn serialize_f32(self, v: f32) -> Result<Value, Error> {
        Ok(Value::Float(v.into()))
    }
    fn serialize_f64(self, v: f64) -> Result<Value, Error> {
        Ok(Value::Float(v))
    }
    fn serialize_char(self, v: char) -> Result<Value, Error> {
        Ok(Value::Str(v.to_string()))
    }
    fn serialize_str(self, v: &str) -> Result<Value, Error> {
        Ok(Value::Str(v.to_owned()))
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<Value, Error> {
        Ok(Value::Bytes(bytes::Bytes::copy_from_slice(v)))
    }
    fn serialize_none(self) -> Result<Value, Error> {
        Ok(Value::Null)
    }
    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<Value, Error> {
        v.serialize(self)
    }
    fn serialize_unit(self) -> Result<Value, Error> {
        Ok(Value::Null)
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<Value, Error> {
        Ok(Value::Null)
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<Value, Error> {
        Ok(Value::Str(variant.to_owned()))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        v: &T,
    ) -> Result<Value, Error> {
        v.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        v: &T,
    ) -> Result<Value, Error> {
        Ok(wrap(Some(variant), v.serialize(ValueSerializer)?))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<SeqSer, Error> {
        Ok(SeqSer {
            items: Vec::with_capacity(len.unwrap_or(0)),
            variant: None,
        })
    }
    fn serialize_tuple(self, len: usize) -> Result<SeqSer, Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> Result<SeqSer, Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<SeqSer, Error> {
        Ok(SeqSer {
            items: Vec::with_capacity(len),
            variant: Some(variant),
        })
    }
    fn serialize_map(self, _: Option<usize>) -> Result<MapSer, Error> {
        Ok(MapSer {
            map: BTreeMap::new(),
            next_key: None,
            variant: None,
        })
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<MapSer, Error> {
        self.serialize_map(None)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<MapSer, Error> {
        Ok(MapSer {
            map: BTreeMap::new(),
            next_key: None,
            variant: Some(variant),
        })
    }
}

impl ser::SerializeSeq for SeqSer {
    type Ok = Value;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
        self.items.push(v.serialize(ValueSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Value, Error> {
        Ok(wrap(self.variant, Value::List(self.items)))
    }
}

impl ser::SerializeTuple for SeqSer {
    type Ok = Value;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
        ser::SerializeSeq::serialize_element(self, v)
    }
    fn end(self) -> Result<Value, Error> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleStruct for SeqSer {
    type Ok = Value;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
        ser::SerializeSeq::serialize_element(self, v)
    }
    fn end(self) -> Result<Value, Error> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleVariant for SeqSer {
    type Ok = Value;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
        ser::SerializeSeq::serialize_element(self, v)
    }
    fn end(self) -> Result<Value, Error> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeMap for MapSer {
    type Ok = Value;
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        self.next_key = Some(key_string(key.serialize(ValueSerializer)?)?);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), Error> {
        let key = self
            .next_key
            .take()
            .ok_or_else(|| Error::Value("map value without a key".into()))?;
        self.map.insert(key, v.serialize(ValueSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Value, Error> {
        Ok(wrap(self.variant, Value::Map(self.map)))
    }
}

impl ser::SerializeStruct for MapSer {
    type Ok = Value;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        self.map
            .insert(key.to_owned(), v.serialize(ValueSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Value, Error> {
        Ok(wrap(self.variant, Value::Map(self.map)))
    }
}

impl ser::SerializeStructVariant for MapSer {
    type Ok = Value;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        ser::SerializeStruct::serialize_field(self, key, v)
    }
    fn end(self) -> Result<Value, Error> {
        ser::SerializeStruct::end(self)
    }
}
