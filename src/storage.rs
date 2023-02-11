use bson::{Document, Bson};
use indexmap::IndexMap;
use parking_lot::RwLock;
use tokio::sync::Notify;
use tokio::time::{self, Duration, Instant};

use std::collections::BTreeMap;
use std::sync::Arc;
use crate::error::EiffelError;
use crate::key::Key;


#[derive(Debug)]
pub struct DbDropGuard {
    datastore: DataStore
}

#[derive(Debug, Clone)]
pub struct DataStore {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    state: RwLock<State>,
    background_task: Notify,
}

#[derive(Debug)]
struct State {

    entries: IndexMap<Key, Entry>,
   
    expirations: BTreeMap<(Instant, u64), Key>,

    // // maybe change u8 to channel oneshot
    // watched_keys: IndexMap<Key, u8>,
   
    next_id: u64,
   
    shutdown: bool,
}


#[derive(Debug)]
struct Entry {
    id: u64,
    data: Arc<Bson>,
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { datastore: DataStore::new() }
    }


    pub(crate) fn datastore(&self) -> DataStore {
        self.datastore.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        self.datastore.shutdown_purge_task();
    }
}

impl DataStore {

    pub fn new() -> DataStore {
        let shared = Arc::new(Shared {
            state: RwLock::new(State {
                entries: IndexMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        tokio::spawn(purge_expired_tasks(shared.clone()));

        DataStore { shared }
    }
  
    // Sets the value at the specified key.
    pub fn set(&self, key: Key, value: Bson) {
        let mut state = self.shared.state.write();

        let id = state.next_id;
        state.next_id += 1;


        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: Arc::new(value),
                expires_at: None,
            },
        );

        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, prev.id));
            }
        }
    }
    
    // Gets the value of a key.
    pub fn get(&self, key: &Key) -> Option<Arc<Bson>> {
    
        let state = self.shared.state.read();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    // Sets the Bson value of a key and return its old value.
    pub fn getset(&self, key: Key, value: Bson) -> Option<Arc<Bson>> {
        let mut state = self.shared.state.write();

        
        let (id, expires_at) = match state.entries.get(&key) {
            Some(entry) => {
                (entry.id, entry.expires_at)
            }
            None => {
                let id = state.next_id;
                state.next_id += 1;

                (id, None)
            }
        };
        

        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: Arc::new(value),
                expires_at,
            },
        ).map(|e| e.data);
        
        drop(state);
        return prev

    }
        
    // Gets the values of all the given keys
    pub fn mget(&self, keys: &[Key]) -> Vec<Arc<Bson>> {
        let mut result = Vec::with_capacity(keys.len());
        let state = self.shared.state.read();
        
        for key in keys {
            if let Some(entry) = state.entries.get(key) {
                result.push(entry.data.clone());
            }
        }

        result
    }

    // Sets the value with the expiry of a key
    pub fn setex(&self, key: Key, value: Bson, expire: Option<Duration>) {
        let mut state = self.shared.state.write();

        
        let id = state.next_id;
        state.next_id += 1;

    
        let mut notify = false;
        let expires_at = expire.map(|duration| {
            
            let when = Instant::now() + duration;
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

        
            state.expirations.insert((when, id), key.clone());
            when
        });

        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: Arc::new(value),
                expires_at,
            },
        );

        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, prev.id));
            }
        }

        drop(state);

        if notify {
            
            self.shared.background_task.notify_one();
        }
    }

    // Sets the value with the expiry of a key, only if the key does not exist
    pub fn setnx(&self, key: Key, value: Bson, expire: Option<Duration>) -> bool {
        let mut state = self.shared.state.write();

        if state.entries.contains_key(&key) {
            return false
        }

        let id = state.next_id;
        state.next_id += 1;


        let mut notify = false;

        let expires_at = expire.map(|duration| {
            let when = Instant::now() + duration;
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            state.expirations.insert((when, id), key.clone());
            when
        });

        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: Arc::new(value),
                expires_at,
            },
        );

        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                
                state.expirations.remove(&(when, prev.id));
            }
        }

        drop(state);

        if notify {
            self.shared.background_task.notify_one();
        }

        return true;
    }

    // Sets multiple keys to multiple values
    pub fn mset(&self, kvs: Vec<(Key, Bson)>) {
        let mut state = self.shared.state.write();

        for (key, value) in kvs {

            let id = state.next_id;
            state.next_id += 1;


            let prev = state.entries.insert(
                key,
                Entry {
                    id,
                    data: Arc::new(value),
                    expires_at: None,
                },
            );

            if let Some(prev) = prev {
                if let Some(when) = prev.expires_at {
                    state.expirations.remove(&(when, prev.id));
                }
            }

        }

    }

    // Sets multiple keys to multiple values, only if none of the keys exist
    pub fn msetnx(&self, kvs: Vec<(Key, Bson)>) -> bool {
        let mut state = self.shared.state.write();

        for (key, _) in kvs.iter() {
            if let Some(_) = state.entries.get(key) {
                return false;
            }
        }

        for (key, value) in kvs {
            let id = state.next_id;
            state.next_id += 1;


            let prev = state.entries.insert(
                key,
                Entry {
                    id,
                    data: Arc::new(value),
                    expires_at: None,
                },
            );

            if let Some(prev) = prev {
                if let Some(when) = prev.expires_at {
                    state.expirations.remove(&(when, prev.id));
                }
            }

        }

        return true;

    }

    // Gets the value by index.
    pub fn cursor(&self, cursor: usize) -> Option<Arc<Bson>> {
        let state = self.shared.state.read();
        state.entries.get_index(cursor).map(|(_, entry)| entry.data.clone())
    }


    // Increments the integer value of a key by one ( This operation is limited to 64 bit signed integers )
    pub fn incr(&self, key: Key) -> Result<i64, EiffelError> {
        let mut state = self.shared.state.write();
        
        match state.entries.get_mut(&key) {
            Some(entry) => {
                match entry.data.as_i64() {
                    None => {
                        return Err(EiffelError::WrongType)
                    }
                    Some(counter) => {
                        let c = counter + 1;
                        entry.data = Arc::new(Bson::Int64(c));
                        return Ok(c);
                    }
                }
            }
            None => {
                
                let id = state.next_id;
                state.next_id += 1;

                state.entries.insert(
                    key,
                    Entry {
                        id,
                        data: Arc::new(Bson::Int64(0)),
                        expires_at: None,
                    },
                );
            
                return Ok(0);
            }
        };


    }


    // Increments the integer value of a key by one ( This operation is limited to 64 bit signed integers )
    pub fn incr_by(&self, key: Key, increment: i64) -> Result<i64, EiffelError> {
        let mut state = self.shared.state.write();
        
        match state.entries.get_mut(&key) {
            Some(entry) => {
                match entry.data.as_i64() {
                    None => {
                        return Err(EiffelError::WrongType)
                    }
                    Some(counter) => {
                        let c = counter + increment;
                        entry.data = Arc::new(Bson::Int64(c));
                        return Ok(c);
                    }
                }
            }
            None => {
                
                let id = state.next_id;
                state.next_id += 1;

                state.entries.insert(
                    key,
                    Entry {
                        id,
                        data: Arc::new(Bson::Int64(increment)),
                        expires_at: None,
                    },
                );
            
                return Ok(increment);
            }
        };


    }


    // Decrements the integer value of a key by one ( This operation is limited to 64 bit signed integers )
    pub fn decr(&self, key: Key) -> Result<i64, EiffelError> {
        let mut state = self.shared.state.write();
        
        match state.entries.get_mut(&key) {
            Some(entry) => {
                match entry.data.as_i64() {
                    None => {
                        return Err(EiffelError::WrongType)
                    }
                    Some(counter) => {
                        let c = counter - 1;
                        entry.data = Arc::new(Bson::Int64(c));
                        return Ok(c);
                    }
                }
            }
            None => {
                
                let id = state.next_id;
                state.next_id += 1;

                state.entries.insert(
                    key,
                    Entry {
                        id,
                        data: Arc::new(Bson::Int64(0)),
                        expires_at: None,
                    },
                );
            
                return Ok(0);
            }
        };


    }


    // Decrements the integer value of a key by the given number ( This operation is limited to 64 bit signed integers )
    pub fn decr_by(&self, key: Key, decrement: i64) -> Result<i64, EiffelError> {
        let mut state = self.shared.state.write();
        
        match state.entries.get_mut(&key) {
            Some(entry) => {
                match entry.data.as_i64() {
                    None => {
                        return Err(EiffelError::WrongType)
                    }
                    Some(counter) => {
                        let c = counter - decrement;
                        entry.data = Arc::new(Bson::Int64(c));
                        return Ok(c);
                    }
                }
            }
            None => {
                
                let id = state.next_id;
                state.next_id += 1;

                state.entries.insert(
                    key,
                    Entry {
                        id,
                        data: Arc::new(Bson::Int64(decrement)),
                        expires_at: None,
                    },
                );
            
                return Ok(decrement);
            }
        };


    }



    pub fn del(&self, key: &Key) {
        let mut state = self.shared.state.write();
        let entry = state.entries.remove(key);
        if let Some(entry) = entry {
            if let Some(when) = entry.expires_at {
                state.expirations.remove(&(when, entry.id));
            }
        }
    }

 
    fn shutdown_purge_task(&self) {

        let mut state = self.shared.state.write();
        state.shutdown = true;

        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.write();

        if state.shutdown {
            return None;
        }

        let state = &mut *state;
        let now = Instant::now();

        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }

            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }

        None
    }

    fn is_shutdown(&self) -> bool {
        self.state.read().shutdown
    }

}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .keys()
            .next()
            .map(|expiration| expiration.0)
    }
}


async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(when) = shared.purge_expired_keys() {
            
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            
            shared.background_task.notified().await;
        }
    }
}