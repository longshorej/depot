extern crate depot;
extern crate jni;
extern crate libc;

use jni::*;
use jni::objects::{GlobalRef, JClass, JString};
use jni::sys::{ JNI_TRUE, JNI_FALSE, jboolean, jint, jlong, jstring, jbyteArray };
use libc::{c_void, c_int};
use depot::queue::{Queue, QueueIterator, QueueItem};
use std::path::Path;
use std::str;
use std::mem;
use std::os::raw::c_char;
use std::path::PathBuf;
use std::slice;
use std::ffi::{CStr,CString};

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueNew(env: JNIEnv,
                                                           _class: JClass,
                                                           path: JString)
                                                         -> jlong {
    let path_string: String = env.get_string(path).unwrap().into();
    let queue = Queue::new(&path_string);

    Box::into_raw(Box::new(queue)) as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueAppend(
    env: JNIEnv,
    _class: JClass,
    queue_ptr: jlong,
    data: jbyteArray
) {
    let queue = &mut *(queue_ptr as *mut Queue);

    let data = env.convert_byte_array(data).unwrap();

    queue.append(&data).unwrap();
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueSync(
    env: JNIEnv,
    _class: JClass,
    queue_ptr: jlong
) {
    let queue = &mut *(queue_ptr as *mut Queue);

    queue.sync().unwrap();
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueIsEmpty(
    env: JNIEnv,
    _class: JClass,
    queue_ptr: jlong
) -> jboolean {
    let queue = &mut *(queue_ptr as *mut Queue);

    match queue.is_empty().unwrap() {
        true  => JNI_TRUE,
        false => JNI_FALSE
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueIsFull(
    env: JNIEnv,
    _class: JClass,
    queue_ptr: jlong
) -> jboolean {
    let queue = &mut *(queue_ptr as *mut Queue);

    match queue.is_full().unwrap() {
        true  => JNI_TRUE,
        false => JNI_FALSE
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueLastId(
    env: JNIEnv,
    _class: JClass,
    queue_ptr: jlong
) -> jint {
    let queue = &mut *(queue_ptr as *mut Queue);

    match queue.last_id().unwrap() {
        Some(id) => id as jint,
        None     => -1
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueDestroy(
    _env: JNIEnv,
    _class: JClass,
    queue_ptr: jlong
){
    let _boxed_queue = Box::from_raw(queue_ptr as *mut Queue);
}


#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueStream(
    _env: JNIEnv,
    _class: JClass,
    queue_ptr: jlong,
    id: jlong
) -> jlong {
    let queue = &mut *(queue_ptr as *mut Queue);

    let id = if id == -1 {
        None
    } else {
        Some(id as u64)
    };

    let stream = queue.stream(id).unwrap();

    Box::into_raw(Box::new(stream)) as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueStreamDestroy(
    _env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong
){
    let _boxed = Box::from_raw(stream_ptr as *mut QueueIterator);
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueStreamItemDestroy(
    _env: JNIEnv,
    _class: JClass,
    item_ptr: jlong
){
    let _boxed = Box::from_raw(item_ptr as *mut Option<(QueueItem, bool)>);
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueStreamNextItem(
    _env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong
) -> jlong {
    let iterator = &mut *(stream_ptr as *mut QueueIterator);

    let next = iterator.next().map(|r| r.unwrap());

    Box::into_raw(Box::new(next)) as jlong
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueStreamItemId(
    _env: JNIEnv,
    _class: JClass,
    item_ptr: jlong
) -> jlong {
    if let Some((item, _)) = &mut *(item_ptr as *mut Option<(QueueItem, bool)>) {
        item.id as i64
    } else {
        -1
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueStreamItemTruncated(
    _env: JNIEnv,
    _class: JClass,
    item_ptr: jlong
) -> jboolean {
    if let Some((_, true)) = &mut *(item_ptr as *mut Option<(QueueItem, bool)>) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueStreamItemLength(
    _env: JNIEnv,
    _class: JClass,
    item_ptr: jlong
) -> jlong {
    if let Some((item, _)) = &mut *(item_ptr as *mut Option<(QueueItem, bool)>) {
        item.data.len() as i64
    } else {
        -1
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "system" fn Java_depot_Native_queueStreamItemCopy(
    env: JNIEnv,
    _class: JClass,
    item_ptr: jlong,
    data: jbyteArray
) {
    if let Some((item, _)) = &mut *(item_ptr as *mut Option<(QueueItem, bool)>) {
        let i8slice = &*((item.data.as_slice()) as *const _  as *const [i8]);

        env.set_byte_array_region(data, 0, i8slice).unwrap();
    }
}

#[test]
fn it_works() {
    assert_eq!(2 + 2, 4);
}

fn copy_to_vec(pointer: *const c_char, length: usize) -> Vec<u8> {
    let slice = unsafe {
        CStr::from_ptr(pointer).to_bytes()
    };

    let mut vec = Vec::with_capacity(length);
    let mut i = 0;

    while i < length {
        vec.push(slice[i]);
        i += 1;
    }

    vec
}

/// Convert a native string to a Rust string
fn to_string(pointer: *const c_char) -> String {
    let slice = unsafe { CStr::from_ptr(pointer).to_bytes() };
    str::from_utf8(slice).unwrap().to_string()
}

/// Convert a Rust string to a native string
fn to_ptr(string: String) -> *const c_char {
    let cs = CString::new(string.as_bytes()).unwrap();
    let ptr = cs.as_ptr();
    // Tell Rust not to clean up the string while we still have a pointer to it.
    // Otherwise, we'll get a segfault.
    mem::forget(cs);
    ptr
}
