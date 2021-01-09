//! Deserialization of binary protocol buffer encoded data.
//!
//! All deserialization operations require a previously loaded set of schema descriptors; see the
//! [`descriptor`](../descriptor/index.html) module for more information.
//!
//! Provided that a set of descriptors have been loaded, a `Deserializer` can be used to deserialize
//! a stream of bytes into something that implements `Deserialize`.
//!
//! ```
//! extern crate serde;
//! extern crate protobuf;
//! extern crate serde_protobuf;
//! extern crate serde_value;
//!
//! use std::fs;
//! use serde::de::Deserialize;
//! use serde_protobuf::descriptor::Descriptors;
//! use serde_protobuf::de::Deserializer;
//! use serde_value::Value;
//!
//! # use std::io;
//! # #[derive(Debug)] struct Error;
//! # impl From<protobuf::ProtobufError> for Error {
//! #   fn from(a: protobuf::ProtobufError) -> Error {
//! #     Error
//! #   }
//! # }
//! # impl From<io::Error> for Error {
//! #   fn from(a: io::Error) -> Error {
//! #     Error
//! #   }
//! # }
//! # impl From<serde_protobuf::error::Error> for Error {
//! #   fn from(a: serde_protobuf::error::Error) -> Error {
//! #     Error
//! #   }
//! # }
//! # impl From<serde_protobuf::error::CompatError> for Error {
//! #   fn from(a: serde_protobuf::error::CompatError) -> Error {
//! #     Error
//! #   }
//! # }
//! # fn foo() -> Result<(), Error> {
//! // Load a descriptor registry (see descriptor module)
//! let mut file = fs::File::open("testdata/descriptors.pb")?;
//! let proto = protobuf::parse_from_reader(&mut file)?;
//! let descriptors = Descriptors::from_proto(&proto);
//!
//! // Set up some data to read
//! let data = &[8, 42];
//! let mut input = protobuf::CodedInputStream::from_bytes(data);
//!
//! // Create a deserializer
//! let name = ".protobuf_unittest.TestAllTypes";
//! let mut deserializer = Deserializer::for_named_message(&descriptors, name, input)?;
//!
//! // Deserialize some struct
//! let value = Value::deserialize(&mut deserializer)?;
//! # println!("{:?}", value);
//! # Ok(())
//! # }
//! # fn main() {
//! #   foo().unwrap();
//! # }
//! ```

use std::io::Read;

use crate::{
    descriptor::{Descriptors, FieldDescriptor, FieldLabel, MessageDescriptor},
    error::CompatResult,
};
use error::CompatError;
use protobuf::CodedInputStream;
use quick_protobuf::BytesReader;
use serde::{self, de::Visitor, forward_to_deserialize_any};

use super::*;
use crate::arraypool::ArrayPool;
use crate::error::{self, Result};
use crate::value::borrowed::*;

/// A deserializer that can deserialize a single message type.
pub struct Deserializer<'de, 'i> {
    inner_builder: DeserializerBuilder<'de>,
    input_buffer: Vec<u8>,
    input: CodedInputStream<'i>,
}

impl<'de, 'i> std::fmt::Debug for Deserializer<'de, 'i> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deserializer").finish()
    }
}

impl<'de, 'i> Deserializer<'de, 'i> {
    /// Constructs a new protocol buffer deserializer for the specified message type.
    ///
    /// The caller must ensure that all of the information needed by the specified message
    /// descriptor is available in the associated descriptors registry.
    pub fn new(
        descriptors: &'de Descriptors,
        descriptor: &'de MessageDescriptor,
        input: CodedInputStream<'i>,
    ) -> Deserializer<'de, 'i> {
        Deserializer {
            inner_builder: DeserializerBuilder::new(descriptors, descriptor),
            input,
            input_buffer: Vec::new(),
        }
    }

    /// Constructs a new protocol buffer deserializer for the specified named message type.
    ///
    /// The message type name must be fully quailified (for example
    /// `".google.protobuf.FileDescriptorSet"`).
    pub fn for_named_message(
        descriptors: &'de Descriptors,
        message_name: &str,
        input: CodedInputStream<'i>,
    ) -> Result<Deserializer<'de, 'i>> {
        if let Some(message) = descriptors.message_by_name(message_name) {
            Ok(Deserializer::new(descriptors, message, input))
        } else {
            Err(error::Error::UnknownMessage {
                name: message_name.to_owned(),
            })
        }
    }
}

impl<'de, 'i> serde::Deserializer<'de> for &'de mut Deserializer<'de, 'i> {
    type Error = CompatError;

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }

    fn deserialize_any<V>(self, visitor: V) -> CompatResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.input_buffer.clear();
        self.input
            .read_to_end(&mut self.input_buffer)
            .map_err(|e| Error::Custom {
                message: format!("error buffering input: {}", e),
            })?;
        self.inner_builder
            .for_input(&self.input_buffer)
            .deserialize_any(visitor)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CurrentMessageDescriptors<'input> {
    pub(crate) current_descriptor: &'input MessageDescriptor,
    pub(crate) all_descriptors: &'input Descriptors,
}

#[derive(Debug)]
pub struct DeserializerBuilder<'descriptors> {
    pool: ArrayPool,
    top_lvl_descriptors: CurrentMessageDescriptors<'descriptors>,
}

impl<'descriptors> DeserializerBuilder<'descriptors> {
    pub fn new(
        descriptors: &'descriptors Descriptors,
        message_descriptor: &'descriptors MessageDescriptor,
    ) -> Self {
        DeserializerBuilder {
            top_lvl_descriptors: CurrentMessageDescriptors {
                all_descriptors: descriptors,
                current_descriptor: message_descriptor,
            },
            pool: ArrayPool::new(),
        }
    }

    pub fn for_named_message(
        descriptors: &'descriptors Descriptors,
        message_name: &str,
    ) -> Result<DeserializerBuilder<'descriptors>> {
        if let Some(current_descriptor) = descriptors.message_by_name(message_name) {
            Ok(DeserializerBuilder::new(descriptors, current_descriptor))
        } else {
            Err(Error::UnknownMessage {
                name: message_name.to_owned(),
            })
        }
    }

    pub fn for_input<'input>(
        &'input mut self,
        input: &'input [u8],
    ) -> impl serde::de::Deserializer<'input, Error = CompatError>
    where
        'descriptors: 'input,
    {
        InnerMessageDeserializer {
            descriptors: self.top_lvl_descriptors,
            input,
            pool: &mut self.pool,
        }
    }
}

struct InnerMessageDeserializer<'input, 'pool> {
    descriptors: CurrentMessageDescriptors<'input>,
    input: &'input [u8],
    pool: &'pool mut ArrayPool,
}

impl<'input, 'pool> serde::Deserializer<'input> for InnerMessageDeserializer<'input, 'pool> {
    type Error = CompatError;

    forward_to_deserialize_any! {
        <V: Visitor<'input>>
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'input>,
    {
        let mut reader = BytesReader::from_bytes(self.input);
        let mut msg = LazliyParsedMessage::parse_from_reader(
            &mut reader,
            self.input,
            self.descriptors,
            self.pool,
        )?;
        let map_visitor = MessageMapVisitor::new(
            self.descriptors,
            msg.single_fields.drain(..),
            msg.repeated_fields.drain(..),
            self.pool,
        );
        let r = visitor.visit_map(map_visitor);
        self.pool.return_field_vec(msg.single_fields);
        self.pool.return_repeated_field_vec(msg.repeated_fields);
        r
    }
}

struct MessageMapVisitor<'input, 'pool, I, RI>
where
    I: Iterator<Item = Field<'input, SingleFieldValue<'input>>>,
    RI: Iterator<Item = Field<'input, RepeatedFieldValue<'input>>>,
{
    descriptors: CurrentMessageDescriptors<'input>,
    single_fields_iterator: I,
    repeated_fields_iterator: RI,
    current: Option<Field<'input, GeneralizedFieldValue<'input>>>,
    pool: &'pool mut ArrayPool,
}

impl<'input, 'pool, I, RI> MessageMapVisitor<'input, 'pool, I, RI>
where
    I: Iterator<Item = Field<'input, SingleFieldValue<'input>>>,
    RI: Iterator<Item = Field<'input, RepeatedFieldValue<'input>>>,
{
    fn new(
        descriptors: CurrentMessageDescriptors<'input>,
        single_fields: I,
        repeated_fields: RI,
        pool: &'pool mut ArrayPool,
    ) -> Self {
        MessageMapVisitor {
            descriptors,
            single_fields_iterator: single_fields,
            repeated_fields_iterator: repeated_fields,
            pool,
            current: None,
        }
    }
}

impl<'input, 'pool, I, RI> serde::de::MapAccess<'input> for MessageMapVisitor<'input, 'pool, I, RI>
where
    I: Iterator<Item = Field<'input, SingleFieldValue<'input>>>,
    RI: Iterator<Item = Field<'input, RepeatedFieldValue<'input>>>,
{
    type Error = CompatError;

    fn next_key_seed<K>(&mut self, seed: K) -> std::result::Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'input>,
    {
        self.current = self
            .single_fields_iterator
            .next()
            .map(|f| Field {
                value: GeneralizedFieldValue::Single(f.value),
                descriptor: f.descriptor,
                tag: f.tag,
            })
            .or_else(|| {
                self.repeated_fields_iterator.next().map(|f| Field {
                    descriptor: f.descriptor,
                    tag: f.tag,
                    value: GeneralizedFieldValue::Repeated(f.value),
                })
            });

        self.current
            .as_ref()
            .map(|c| {
                seed.deserialize(MessageKeyDeserializer {
                    key: c.descriptor.name(),
                })
            })
            .transpose()
    }

    fn next_value_seed<V>(&mut self, seed: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'input>,
    {
        let field = self
            .current
            .take()
            .expect("visit_value called before visit_key");

        let field_deserializer = MessageFieldDeserializer {
            descriptors: self.descriptors,
            field: &field,
            pool: self.pool,
        };
        let result = seed.deserialize(field_deserializer);

        if let GeneralizedFieldValue::Repeated(vec) = field.value {
            self.pool.return_single_field_val_vec(vec);
        }

        result
    }
}

struct MessageKeyDeserializer<'input> {
    key: &'input str,
}

impl<'input> serde::Deserializer<'input> for MessageKeyDeserializer<'input> {
    forward_to_deserialize_any! {
        <V: Visitor<'input>>
      bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
    byte_buf option unit unit_struct newtype_struct seq tuple
    tuple_struct map struct enum identifier ignored_any
    }

    type Error = CompatError;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'input>,
    {
        visitor.visit_borrowed_str(self.key)
    }
}

struct MessageFieldDeserializer<'input, 'pool, 'internal> {
    descriptors: CurrentMessageDescriptors<'input>,
    field: &'internal Field<'input, GeneralizedFieldValue<'input>>,
    pool: &'pool mut ArrayPool,
}

impl<'input, 'pool, 'internal> serde::Deserializer<'input>
    for MessageFieldDeserializer<'input, 'pool, 'internal>
where
    'input: 'pool,
    'pool: 'internal,
{
    type Error = CompatError;

    forward_to_deserialize_any! {
        <V: Visitor<'input>>
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'input>,
    {
        match &self.field.value {
            GeneralizedFieldValue::Single(value) => {
                if self.field.descriptor.field_label() == FieldLabel::Optional {
                    visitor.visit_some(ValueDeserializer {
                        descriptors: self.descriptors,
                        field: value,
                        field_descriptor: self.field.descriptor,
                        pool: self.pool,
                    })
                } else {
                    visit_value(
                        self.descriptors.all_descriptors,
                        self.field.descriptor,
                        value,
                        self.pool,
                        visitor,
                    )
                }
            }
            GeneralizedFieldValue::Repeated(vec) => visitor.visit_seq(RepeatedValueVisitor {
                descriptors: self.descriptors,
                field_descriptor: self.field.descriptor,
                fields_iter: vec.iter(),
                pool: self.pool,
            }),
        }
    }
}

struct RepeatedValueVisitor<'input, 'pool, 'internal, I>
where
    'input: 'internal,
    I: Iterator<Item = &'internal SingleFieldValue<'input>>,
{
    descriptors: CurrentMessageDescriptors<'input>,
    field_descriptor: &'internal FieldDescriptor,
    fields_iter: I,
    pool: &'pool mut ArrayPool,
}

impl<'input, 'pool, 'internal, I> serde::de::SeqAccess<'input>
    for RepeatedValueVisitor<'input, 'pool, 'internal, I>
where
    'input: 'internal,
    I: Iterator<Item = &'internal SingleFieldValue<'input>>,
{
    type Error = CompatError;

    fn next_element_seed<T>(&mut self, seed: T) -> CompatResult<Option<T::Value>>
    where
        T: serde::de::DeserializeSeed<'input>,
    {
        self.fields_iter
            .next()
            .map(|field| {
                seed.deserialize(ValueDeserializer {
                    descriptors: self.descriptors,
                    field_descriptor: self.field_descriptor,
                    field,
                    pool: self.pool,
                })
            })
            .transpose()
    }

    fn size_hint(&self) -> Option<usize> {
        self.fields_iter.size_hint().1
    }
}

struct ValueDeserializer<'input, 'pool, 'internal> {
    descriptors: CurrentMessageDescriptors<'input>,
    field_descriptor: &'internal FieldDescriptor,
    field: &'internal SingleFieldValue<'input>,
    pool: &'pool mut ArrayPool,
}

impl<'de, 'pool, 'internal> serde::Deserializer<'de> for ValueDeserializer<'de, 'pool, 'internal> {
    type Error = CompatError;

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }

    fn deserialize_any<V>(self, visitor: V) -> CompatResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visit_value(
            self.descriptors.all_descriptors,
            self.field_descriptor,
            self.field,
            self.pool,
            visitor,
        )
    }
}

fn visit_value<'input, V>(
    all_descriptors: &'input Descriptors,
    field_descriptor: &FieldDescriptor,
    value: &SingleFieldValue<'input>,
    pool: &mut ArrayPool,
    visitor: V,
) -> CompatResult<V::Value>
where
    V: Visitor<'input>,
{
    match *value {
        SingleFieldValue::Bool(v) => visitor.visit_bool(v),
        SingleFieldValue::I32(v) => visitor.visit_i32(v),
        SingleFieldValue::I64(v) => visitor.visit_i64(v),
        SingleFieldValue::U32(v) => visitor.visit_u32(v),
        SingleFieldValue::U64(v) => visitor.visit_u64(v),
        SingleFieldValue::F32(v) => visitor.visit_f32(v),
        SingleFieldValue::F64(v) => visitor.visit_f64(v),
        SingleFieldValue::Bytes(v) => visitor.visit_borrowed_bytes(v),
        SingleFieldValue::String(v) => visitor.visit_borrowed_str(v),
        SingleFieldValue::Enum(e) => {
            visit_enum_value(e, field_descriptor, all_descriptors, visitor)
        }
        SingleFieldValue::LazyMessage { descriptor, data } => {
            let deserializer = InnerMessageDeserializer {
                descriptors: CurrentMessageDescriptors {
                    all_descriptors,
                    current_descriptor: descriptor,
                },
                input: data,
                pool,
            };
            serde::de::Deserializer::deserialize_any(deserializer, visitor)
        }
        SingleFieldValue::Null => visitor.visit_none(),
        SingleFieldValue::BorrowedDefaultValue { inner } => match inner {
            value::Value::Bool(b) => visitor.visit_bool(*b),
            value::Value::I32(v) => visitor.visit_i32(*v),
            value::Value::I64(v) => visitor.visit_i64(*v),
            value::Value::U32(v) => visitor.visit_u32(*v),
            value::Value::U64(v) => visitor.visit_u64(*v),
            value::Value::F32(v) => visitor.visit_f32(*v),
            value::Value::F64(v) => visitor.visit_f64(*v),
            value::Value::Bytes(b) => visitor.visit_borrowed_bytes(&b),
            value::Value::String(s) => visitor.visit_borrowed_str(&s),
            value::Value::Enum(e) => {
                visit_enum_value(*e, field_descriptor, all_descriptors, visitor)
            }
            value::Value::Message(_) => panic!("unsupported default message _value_"),
        },
    }
}

fn visit_enum_value<'input, V>(
    value: i32,
    field_descriptor: &FieldDescriptor,
    all_descriptors: &'input Descriptors,
    visitor: V,
) -> CompatResult<V::Value>
where
    V: Visitor<'input>,
{
    if let descriptor::FieldType::Enum(d) = field_descriptor.field_type(all_descriptors) {
        d.value_by_number(value)
            .ok_or_else(|| Error::UnknownEnumValue { value }.into())
            .and_then(|enum_descriptor| visitor.visit_str(enum_descriptor.name()))
    } else {
        Err(Error::Custom {
            message: "field and wire type mismatch".to_string(),
        }
        .into())
    }
}
