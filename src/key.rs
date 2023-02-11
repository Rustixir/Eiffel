
use bson::{oid::ObjectId, Uuid};



#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Key {
    Uint32(u32),
    Uint64(u64),
    String(String),
    Uuid(Uuid),
    ObjectId(ObjectId),
}   


pub trait ToKey {
    fn to_key(&self) -> Key;
}



impl ToKey for i32 {
    fn to_key(&self) -> Key {
        Key::Uint32(*self as u32)
    }
}
impl ToKey for u32 {
    fn to_key(&self) -> Key {
        Key::Uint32(*self)
    }
}


impl ToKey for i64 {
    fn to_key(&self) -> Key {
        Key::Uint64(*self as u64)
    }
}
impl ToKey for u64 {
    fn to_key(&self) -> Key {
        Key::Uint64(*self)
    }
}


impl ToKey for String {
    fn to_key(&self) -> Key {
        Key::String(self.to_owned())
    }
}

impl ToKey for &str {
    fn to_key(&self) -> Key {
        Key::String(self.to_string())
    }
}


impl ToKey for ObjectId {
    fn to_key(&self) -> Key {
        Key::ObjectId(self.to_owned())
    }
}


impl ToKey for Uuid {
    fn to_key(&self) -> Key {
        Key::Uuid(self.to_owned())
    }
}
