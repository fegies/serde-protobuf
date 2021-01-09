use core::panic;

use crate::descriptor::{Descriptors, FieldDescriptor, FieldLabel, FieldType, MessageDescriptor};
use quick_protobuf::BytesReader;

use crate::{
    arraypool::ArrayPool, de::CurrentMessageDescriptors, error::Result, util::retain_last_by_key,
};

use super::owned;

pub enum SingleFieldValue<'input> {
    /// A boolean value.
    Bool(bool),
    /// A 32-bit signed integer.
    I32(i32),
    /// A 64-bit signed integer.
    I64(i64),
    /// A 32-bit unsigned integer.
    U32(u32),
    /// A 64-bit unsigned integer.
    U64(u64),
    /// A 32-bit floating point value.
    F32(f32),
    /// A 64-bit floating point value.
    F64(f64),
    /// A byte vector.
    Bytes(&'input [u8]),
    /// A string.
    String(&'input str),
    /// An enum value.
    Enum(i32),
    /// A message.
    LazyMessage {
        descriptor: &'input MessageDescriptor,
        data: &'input [u8],
    },
    /// A null value (message default value)
    Null,
    /// A Default value borrowed from the descriptor
    BorrowedDefaultValue { inner: &'input owned::Value },
}

pub type RepeatedFieldValue<'input> = Vec<SingleFieldValue<'input>>;

pub enum GeneralizedFieldValue<'input> {
    Single(SingleFieldValue<'input>),
    Repeated(RepeatedFieldValue<'input>),
}

pub struct Field<'input, V> {
    pub descriptor: &'input FieldDescriptor,
    pub value: V,
    pub tag: u32,
}

impl<'input> SingleFieldValue<'input> {
    fn parse_from_reader(
        reader: &mut BytesReader,
        bytes: &'input [u8],
        field_type: &FieldType<'input>,
    ) -> Result<Self> {
        let r = match field_type {
            FieldType::UnresolvedMessage(name) => Err(crate::Error::UnknownMessage {
                name: name.to_string(),
            })?,
            FieldType::UnresolvedEnum(name) => Err(crate::Error::UnknownEnum {
                name: name.to_string(),
            })?,
            FieldType::Double => SingleFieldValue::F64(reader.read_double(bytes)?),
            FieldType::Float => SingleFieldValue::F32(reader.read_float(bytes)?),
            FieldType::Int64 => SingleFieldValue::I64(reader.read_int64(bytes)?),
            FieldType::UInt64 => SingleFieldValue::U64(reader.read_uint64(bytes)?),
            FieldType::Int32 => SingleFieldValue::I32(reader.read_int32(bytes)?),
            FieldType::Fixed64 => SingleFieldValue::U64(reader.read_fixed64(bytes)?),
            FieldType::Fixed32 => SingleFieldValue::U32(reader.read_fixed32(bytes)?),
            FieldType::Bool => SingleFieldValue::Bool(reader.read_bool(bytes)?),
            FieldType::String => SingleFieldValue::String(reader.read_string(bytes)?),
            FieldType::Group => Err(crate::Error::Custom {
                message: "Groups are not supported".to_string(),
            })?,
            FieldType::Bytes => SingleFieldValue::Bytes(reader.read_bytes(bytes)?),
            FieldType::UInt32 => SingleFieldValue::U32(reader.read_uint32(bytes)?),
            FieldType::Enum(_) => SingleFieldValue::Enum(reader.read_enum::<i32>(bytes)?),
            FieldType::SFixed32 => SingleFieldValue::I32(reader.read_sfixed32(bytes)?),
            FieldType::SFixed64 => SingleFieldValue::I64(reader.read_sfixed64(bytes)?),
            FieldType::SInt32 => SingleFieldValue::I32(reader.read_sint32(bytes)?),
            FieldType::SInt64 => SingleFieldValue::I64(reader.read_sint64(bytes)?),
            FieldType::Message(descriptor) => SingleFieldValue::LazyMessage {
                descriptor,
                data: reader.read_bytes(bytes)?,
            },
        };
        Ok(r)
    }
}

pub struct LazliyParsedMessage<'input> {
    pub single_fields: Vec<Field<'input, SingleFieldValue<'input>>>,
    pub repeated_fields: Vec<Field<'input, RepeatedFieldValue<'input>>>,
}

fn compute_default_value_for_field<'input>(
    field_descriptor: &'input FieldDescriptor,
    descriptors: &Descriptors,
) -> Field<'input, SingleFieldValue<'input>> {
    let value = field_descriptor
        .default_value()
        .map(|inner| SingleFieldValue::BorrowedDefaultValue { inner })
        .or_else(|| {
            if field_descriptor.field_label() == FieldLabel::Optional {
                Some(SingleFieldValue::Null)
            } else {
                None
            }
        })
        .unwrap_or_else(|| match field_descriptor.field_type(descriptors) {
            FieldType::UnresolvedMessage(_) => {
                panic!("unresolved message default value")
            }
            FieldType::UnresolvedEnum(_) => {
                panic!("unresolved enum default value")
            }
            FieldType::Double => SingleFieldValue::F64(Default::default()),
            FieldType::Float => SingleFieldValue::F32(Default::default()),
            FieldType::Int64 => SingleFieldValue::I64(Default::default()),
            FieldType::UInt64 => SingleFieldValue::U64(Default::default()),
            FieldType::Int32 => SingleFieldValue::I32(Default::default()),
            FieldType::Fixed64 => SingleFieldValue::U64(Default::default()),
            FieldType::Fixed32 => SingleFieldValue::U32(Default::default()),
            FieldType::Bool => SingleFieldValue::Bool(Default::default()),
            FieldType::String => SingleFieldValue::String(Default::default()),
            FieldType::Group => {
                panic!("group default value")
            }
            FieldType::Message(_) => SingleFieldValue::Null,
            FieldType::Bytes => SingleFieldValue::Bytes(Default::default()),
            FieldType::UInt32 => SingleFieldValue::U32(Default::default()),
            FieldType::Enum(_) => SingleFieldValue::Enum(Default::default()),
            FieldType::SFixed32 => SingleFieldValue::I32(Default::default()),
            FieldType::SFixed64 => SingleFieldValue::I64(Default::default()),
            FieldType::SInt32 => SingleFieldValue::I32(Default::default()),
            FieldType::SInt64 => SingleFieldValue::I64(Default::default()),
        });
    Field {
        tag: field_descriptor.number() as u32,
        descriptor: field_descriptor,
        value,
    }
}

impl<'input> LazliyParsedMessage<'input> {
    pub(crate) fn parse_from_reader(
        reader: &mut BytesReader,
        bytes: &'input [u8],
        descriptors: CurrentMessageDescriptors<'input>,
        pool: &mut ArrayPool,
    ) -> Result<Self> {
        let mut single_fields = pool.get_field_vec();
        let mut repeated_fields: Vec<Field<RepeatedFieldValue>> = pool.get_repeated_field_vec();

        single_fields.extend(
            descriptors
                .current_descriptor
                .fields()
                .iter()
                .filter(|f| !f.is_repeated())
                .map(|d| compute_default_value_for_field(d, descriptors.all_descriptors)),
        );
        repeated_fields.extend(
            descriptors
                .current_descriptor
                .fields()
                .iter()
                .filter(|f| f.is_repeated())
                .map(|d| Field {
                    value: pool.get_single_field_val_vec(),
                    tag: d.number() as u32,
                    descriptor: d,
                }),
        );

        while !reader.is_eof() {
            let tag = reader.next_tag(bytes)?;
            let field_number = tag >> 3;
            match descriptors
                .current_descriptor
                .fields()
                .iter()
                .find(|f| f.number() == field_number as i32)
            {
                Some(descriptor) => {
                    let value = SingleFieldValue::parse_from_reader(
                        reader,
                        bytes,
                        &descriptor.field_type(descriptors.all_descriptors),
                    )?;
                    if descriptor.is_repeated() {
                        match repeated_fields.iter_mut().find(|f| f.tag == field_number) {
                            Some(v) => v.value.push(value),
                            None => {
                                repeated_fields.push(Field {
                                    descriptor,
                                    tag: field_number,
                                    value: pool.get_single_field_val_vec(),
                                });
                                repeated_fields.last_mut().unwrap().value.push(value);
                            }
                        };
                    } else {
                        single_fields.push(Field {
                            descriptor,
                            tag: field_number,
                            value,
                        });
                    }
                }
                // TODO: actually store the unknown field
                None => {
                    reader.read_unknown(bytes, tag)?;
                }
            }
        }
        single_fields.sort_by_key(|f| f.tag);
        retain_last_by_key(&mut single_fields, |f| f.tag);
        Ok(LazliyParsedMessage {
            single_fields,
            repeated_fields,
        })
    }
}
