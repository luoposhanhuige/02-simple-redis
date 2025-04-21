use crate::RespFrame;
use dashmap::DashMap;
use std::ops::Deref;
use std::sync::Arc;

// The backend.rs file defines a backend storage system for your Redis-like application.
// It provides functionality to store, retrieve, and manage key-value pairs and hash maps, mimicking the behavior of a Redis backend.

#[derive(Debug, Clone)]
pub struct Backend(Arc<BackendInner>);

#[derive(Debug)]
pub struct BackendInner {
    pub(crate) map: DashMap<String, RespFrame>,
    pub(crate) hmap: DashMap<String, DashMap<String, RespFrame>>,
}

impl Deref for Backend {
    type Target = BackendInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Backend {
    fn default() -> Self {
        Self(Arc::new(BackendInner::default()))
    }
}

impl Default for BackendInner {
    fn default() -> Self {
        Self {
            map: DashMap::new(),
            hmap: DashMap::new(),
        }
    }
}

impl Backend {
    pub fn new() -> Self {
        Self::default()
    }

    // &self
    // The method takes an immutable reference to self, meaning it does not modify the Backend instance.
    // This allows multiple threads or parts of the program to call get concurrently, as long as no mutation occurs.
    // Using &str instead of String avoids unnecessary allocations because &str is a borrowed reference to an existing string, while String is an owned type that requires memory allocation.

    pub fn get(&self, key: &str) -> Option<RespFrame> {
        self.map.get(key).map(|v| v.value().clone()) // Deref is involved here.
                                                     // self.map is a DashMap<String, RespFrame>, which is a thread-safe hash map.
                                                     // The get method of DashMap is used to retrieve a reference to the value associated with the given key.
                                                     // If the key exists, it returns Some(Ref<'_, V>), where Ref is a wrapper around the value (RespFrame) that ensures thread-safe access.
                                                     // The map method is called on the Option returned by self.map.get(key).

        // self.map.get(key) returns Some(v)
        // v is a Ref<'_, RespFrame>, which is a thread-safe reference to the value.
        // v.value() extracts the underlying RespFrame from the Ref.
        // .clone() creates a deep copy of the RespFrame so that the caller gets ownership of the value.

        // Ref is a type provided by DashMap to ensure safe access to the value in a concurrent environment. It is essentially a smart pointer that wraps the value and ensures that:
        // The value is not modified while it is being accessed.
        // Multiple threads can safely read the value concurrently.

        // v is of type Ref<'_, RespFrame>.
        // The value() method of Ref returns a reference to the RespFrame stored in the DashMap.
        // .clone():
        // Since v.value() returns a reference (&RespFrame), calling .clone() creates a deep copy of the RespFrame.
        // This ensures that the caller gets ownership of the value without affecting the original value in the DashMap.
    }

    // The reason the set function does not include Option<RespFrame> in its return type is that the current implementation chooses to ignore the return value of the DashMap::insert method.
    pub fn set(&self, key: String, value: RespFrame) {
        self.map.insert(key, value);
    }
    // Return Scenarios
    // If the Key Already Exists:
    // The method replaces the old value with the new one.
    // It returns Some(old_value), where old_value is the previous value associated with the key.
    // If the Key Does Not Exist:
    // The method inserts the new key-value pair.
    // It returns None.

    // DashMap is a concurrent hash map that allows multiple threads to read and write to the map without locking the entire map.
    // if you use traditional hashmap, the following .get(key) would not return Option<Ref<'_, V>>.
    // Instead, it would return Option<&V>, which is a reference to the value.
    // The .get(key) method of DashMap returns an Option<Ref<'_, V>>, where Ref is a smart pointer that provides safe access to the value.
    // The Ref type is used to ensure that the value is not modified while it is being accessed, and it allows for safe concurrent access.
    // The Ref type is a smart pointer that provides safe access to the value in a concurrent environment.

    // 以下几个关键点，
    // DashMap:     .hmap.get(key) 是直接作用于 DashMap<String, DashMap<String, RespFrame>> 的
    // Option:      .and_then 与 .map 是作用于 Option<Ref<'_, String, ...>> 的
    // Deref:       v.get(field) 与 v.value() 是作用于 Ref<'_, DashMap<String, RespFrame>> 的，用到 Deref trait。

    // .and_then 不能被 .map 替代，因为前者中的 F: FnOnce(T) -> Option<U>，作为整个.and_then 的返回，
    // .map 中的 F: Fn(&T) -> U，然后返回 Some(f(x))，相当于在 Some(U)，
    // .map(.map()) 嵌套的话，返回值是 Option<Option<U>>，而不是 Option<U>。

    // .and_then 内部的 f 的返回值是 Option<U>，
    // .map 内部的 f 的返回值是 U，但 .map 这个 function 的返回值是 Option<U>，所以，需要把 f 的返回值做个处理，类似 Some(f(x))，而不是直接返回 f(x)。
    // .and_then(.map()) 嵌套的话，因为内部 .map() 返回一个 Option<U>，编译器对比该 Option<U> 和 .and_then 整体的返回值一致，所以通过。

    // 如果 .map(.map())，返回值就是 Option<Option<U>>，这个时候，需要调用 .flatten()，把 Option<Option<U>> 转换成 Option<U>。

    // 所以，.and_then(.map()) 可以嵌套使用。
    pub fn hget(&self, key: &str, field: &str) -> Option<RespFrame> {
        self.hmap
            .get(key) // Since DashMap directly provides the get method, no Deref is involved here.
            .and_then(|v| {
                v.get(field) // Deref is involved. v is not directly a DashMap<String, RespFrame>; it is a Ref<'_, DashMap<String, RespFrame>>.
                    .map(
                        |v| {
                            v.value() // Deref is involved too.
                                .clone()
                        }, // returns a copy of the RespFrame instance, not a reference to it.
                    )
            })
    }

    // The purpose of this code is to:
    // Retrieve the inner DashMap<String, RespFrame> associated with the given key in the outer DashMap.
    // If the key does not exist in the outer DashMap, create a new, default DashMap<String, RespFrame> and insert it into the outer DashMap.
    // This ensures that the key always has an associated DashMap<String, RespFrame> for storing field-value pairs.

    // 关于变量名，令人混淆这件事：
    // Yes, you are absolutely correct!
    // The name hmap in the hset function can indeed be confusing because it shadows the hmap field of BackendInner.
    // While the hmap field in BackendInner refers to the entire outer DashMap<String, DashMap<String, RespFrame>>,
    // the hmap variable in the hset function refers to an entry (or more specifically,
    //     a reference to the inner DashMap<String, RespFrame> associated with a specific key in the outer DashMap).

    // Yes, changing the name from hmap to hmap_entry (or something similar) would be better because it makes the code more descriptive and avoids confusion between the hmap field of BackendInner and the local variable in the hset function.
    pub fn hset(&self, key: String, field: String, value: RespFrame) {
        // 下面这个变量名令人产生歧义，修改为 hmap_entry 更好
        let hmap = self.hmap.entry(key).or_default(); // .entry 返回一个 Enum Entry，然后 Entry.or_default 返回 RefMut<'a, K, V>
        hmap.insert(field, value);
        // RefMut<'_, K, V> is a DashMap-exclusive type, not something from the standard library.
        // RefMut<'_, String, DashMap<..., ...>>, is because RefMut implements the DerefMut trait,
        // which allows it to behave like the underlying DashMap when accessing its methods.
        // When you call hmap.insert(...),
        // Rust sees that hmap is a RefMut and automatically dereferences it to the underlying DashMap to resolve the method call.
    }

    // impl<'a, K: Eq + Hash, V> DerefMut for RefMut<'a, K, V> {
    //     fn deref_mut(&mut self) -> &mut V {
    //         self.value_mut()
    //     }
    // }

    // in the context of DashMap, an entry refers to the pair of a key and its associated value in the map. The entry API provides a way to access or modify a key-value pair in the map, whether the key already exists or not. It allows you to work with the map in a more flexible way compared to just using get or insert.

    // 1. What Does entry Mean in DashMap?
    // The entry method in DashMap is used to access or create an entry for a given key. It represents the state of the key in the map:

    // If the key exists, the entry is considered occupied.
    // If the key does not exist, the entry is considered vacant.
    // The entry method returns an Entry enum, which can be either:

    // Entry::Occupied: Represents an existing key-value pair in the map.
    // Entry::Vacant: Represents a key that does not yet exist in the map.

    // pub fn entry(&'a self, key: K) -> Entry<'a, K, V> {
    //     self._entry(key)
    // }

    // _entry(key)返回的是：
    // {
    //     Ok(elem) => Entry::Occupied(unsafe { OccupiedEntry::new(shard, key, elem) }),
    //     Err(slot) => Entry::Vacant(unsafe { VacantEntry::new(shard, key, hash, slot) }),
    // }

    // pub enum Entry<'a, K, V> {
    //     Occupied(OccupiedEntry<'a, K, V>),
    //     Vacant(VacantEntry<'a, K, V>),
    // }

    // pub fn or_default(self) -> RefMut<'a, K, V>
    // where
    //     V: Default,
    // {
    //     match self {
    //         Entry::Occupied(entry) => entry.into_ref(),
    //         Entry::Vacant(entry) => entry.insert(V::default()),
    //     }
    // }

    pub fn hgetall(&self, key: &str) -> Option<DashMap<String, RespFrame>> {
        self.hmap.get(key).map(|v| v.clone())
    }
}
