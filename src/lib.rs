//!< Module for ringbuffers.

use cache_padded::CachePadded;
use std::cell::UnsafeCell;
use std::default::Default;
use std::marker::Sized;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use thiserror::Error;

/// Defines errors types for ringbuffer functionality.
#[derive(Copy, Clone, Debug, Error)]
pub enum RingbufferError {
    /// The buffer is full, and can't be pushed into.
    #[error("The buffer is full")]
    BufferIsFull,
    /// The buffe ris empty, and can't be popped from.
    #[error("The buffer is empty")]
    BufferIsEmpty,
}

/// Alias for a concurrent ringbuffer which *does not* perform padding
/// to avoid false sharing. This is best used when contention on the buffer
/// is low and/or the data elements are small, and therefore the cache alignment
/// will cause significant memory bloat.
///
/// The correct way to use this is through an Arc, and then to use the
/// producer and consumer methods to create producers and consumers for the
/// buffer.
///
/// # Threading
///
/// This is threadsafe. Any number of producers and consumers can be used across
/// any number of threads to push into and pop off of the buffer, respectively.
///
/// # Examples
///
/// ```
/// use circlet::{UnpaddedRingbuffer, channel};
/// use std::sync::Arc;
///
/// let buffer = Arc::new(UnpaddedRingbuffer::new(10));
///
/// let (mut prod, mut cons) = channel(buffer.clone());
///
/// let mut val : u32 = 11;
/// prod.push(val);
/// cons.pop(&mut val);
/// ```
pub type UnpaddedRingbuffer<T> = Ringbuffer<UnpaddedSlot<T>>;

/// Alias type for a concurrent ringbuffer which *does* perform padding
/// to avoid false sharing. This is best used when contention on the buffer
/// is high and/or the data elements are large (around the cacheline size) so
/// that the cachline alignment does not cause signifcant memory bloat.
///
/// The correct way to use this is through an Arc, and then to use the
/// producer and consumer methods to create producers and consumers for the
/// buffer.
///
/// # Threading
///
/// This is threadsafe. Any number of producers and consumers can be used across
/// any number of threads to push into and pop off of the buffer, respectively.
///
/// # Examples
///
/// ```
/// use circlet::{PaddedRingbuffer, channel};
/// use std::sync::Arc;
///
/// let buffer = Arc::new(PaddedRingbuffer::new(10));
///
/// let (mut prod, mut cons) = channel(buffer.clone());
///
/// let mut val = 11;
/// prod.push(val);
/// cons.pop(&mut val);
/// ```
pub type PaddedRingbuffer<T> = Ringbuffer<PaddedSlot<T>>;

/// Creates a producer for the `ringbuffer`.
pub fn producer<T>(ringbuffer: Arc<Ringbuffer<T>>) -> Producer<T> {
    Producer { buffer: ringbuffer }
}

/// Creates a consumer for the `ringbuffer`.
pub fn consumer<T>(ringbuffer: Arc<Ringbuffer<T>>) -> Consumer<T> {
    Consumer { buffer: ringbuffer }
}

/// Creates a channel for the buffer, consisting of a producer and consumer.
pub fn channel<T>(ringbuffer: Arc<Ringbuffer<T>>) -> (Producer<T>, Consumer<T>) {
    (producer(ringbuffer.clone()), consumer(ringbuffer))
}

/// A trait used to define the properties required for slots in a ringbuffer.
///
/// A slot is simply a wrapper around some data with a tag to avoid race
/// conditions when wrapping around the end of the ringbuffer storage. This trait
/// allows for the generic implemetation of a concurrent ringbuffer with slots
/// which are either cache aligned or not cache aligned. See [UnpaddedSlot] and
/// [PaddedSlot] for more details.
pub trait Slot {
    /// The type of the storage for the slot.
    type Storage;

    /// Returns a mutable reference to the slot storage.
    fn storage_mut(&mut self) -> &mut Self::Storage;

    /// Returns a copy of the slot storage.
    fn storage(self) -> Self::Storage;

    /// Returns a reference to the tag for the slot.
    fn tag(&mut self) -> &mut AtomicU32;

    /// Returns the value of the tag with acquire ordering.
    fn acquire_tag(&self) -> u32;

    /// Creates a new slot from the storage.
    fn new() -> Self;
}

/// A slot in the concurrent ring buffer. This slot *does not* perform cacheline
/// alignment of the data, and thus *will not prevent* false sharing between
/// threads.
///
/// It is best to benchmark to determine if false sharing causes performance
/// degredation and/or if the memory bloat is acceptable, and if not, then use
/// a [PaddedSlot] instead.
///
/// This type is not designed to be used directly. Rather, [UnpaddedRingbuffer]
/// should be used.
pub struct UnpaddedSlot<T> {
    /// The data for the slot.
    pub(crate) data: T,
    /// Tag used to determine determine correct access to the slot.
    pub(crate) tag: AtomicU32,
}

impl<T: Sized + Default + Copy> UnpaddedSlot<T> {
    /// Creates a new unpadded slot.
    pub fn new(t: T) -> UnpaddedSlot<T> {
        UnpaddedSlot {
            data: t,
            tag: AtomicU32::new(0),
        }
    }
}

impl<T: Sized + Default + Copy> Slot for UnpaddedSlot<T> {
    type Storage = T;

    fn storage_mut(&mut self) -> &mut T {
        &mut self.data
    }

    fn storage(self) -> T {
        self.data
    }

    fn tag(&mut self) -> &mut AtomicU32 {
        &mut self.tag
    }

    fn acquire_tag(&self) -> u32 {
        self.tag.load(Ordering::Acquire)
    }

    fn new() -> Self {
        UnpaddedSlot::new(T::default())
    }
}

/// A cache padded slot in the concurrent ring buffer. This slot *does* perform
/// cacheline alignment of the data, and thus *will prevent* false sharing
/// between threads.
///
/// The overhead of this for some data types might be too high for some data,
/// (i.e ones which are small, since the padding in such a case could cause
/// significant waste), or for access patterns where contention is very low,
/// in which case [UnpaddedSlot] would be a better fit.
///
/// This type is not designed to be used directly. Rather, [PaddedRingbuffer]
/// should be used.
pub struct PaddedSlot<T> {
    /// The slot implementation
    pub(crate) slot: CachePadded<UnpaddedSlot<T>>,
}

impl<T: Sized + Default + Copy> PaddedSlot<T> {
    /// Creates a new unpadded slot.
    pub fn new(t: T) -> PaddedSlot<T> {
        PaddedSlot {
            slot: CachePadded::new(UnpaddedSlot::new(t)),
        }
    }
}

impl<T: Sized + Default + Copy> Slot for PaddedSlot<T> {
    type Storage = T;

    fn storage_mut(&mut self) -> &mut T {
        &mut self.slot.data
    }

    fn storage(self) -> T {
        self.slot.data
    }

    fn tag(&mut self) -> &mut AtomicU32 {
        &mut self.slot.tag
    }

    fn acquire_tag(&self) -> u32 {
        self.slot.tag.load(Ordering::Acquire)
    }

    fn new() -> Self {
        PaddedSlot::new(T::default())
    }
}

/// Stores slots for a ringbuffer which can be shared between threads.
pub(crate) struct SharableSlots<T: Sized> {
    /// The slots which are sharable between threads.
    slots: UnsafeCell<Vec<T>>,
}

unsafe impl<T: Sized> Sync for SharableSlots<T> {}

impl<T: Sized> SharableSlots<T> {
    /// Creates new sharable slots from the `raw_slots`.
    pub fn new(raw_slots: Vec<T>) -> Self {
        Self {
            slots: UnsafeCell::new(raw_slots),
        }
    }

    /// Gets a reference to the vector of slots.
    pub unsafe fn get(&self) -> &Vec<T> {
        &*self.slots.get()
    }

    /// Gets a mutable reference to the vector of slots.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get_mut(&self) -> &mut Vec<T> {
        &mut *self.slots.get()
    }
}

/// A concurrrent, fixed-size multi-producer multi-consumer queue.
pub struct Ringbuffer<T> {
    /// Slots for the buffer.
    pub(crate) slots: SharableSlots<T>,
    /// Capacity of the buffer.
    pub(crate) capacity: u32,
    /// Index of the head of the buffer.
    pub(crate) head: CachePadded<AtomicU32>,
    /// Index of the tail of the buffer.
    pub(crate) tail: CachePadded<AtomicU32>,
}

impl<T: Slot> Ringbuffer<T>
where
    <T as Slot>::Storage: Copy,
{
    /// Creates a new concurrent ringbuffer with space for `capacity` elements.
    /// This is *not* threadsafe.
    pub fn new(capacity: u32) -> Ringbuffer<T> {
        let mut slots = Vec::with_capacity(capacity as usize);
        for _ in 0..capacity {
            slots.push(T::new());
        }

        Ringbuffer {
            slots: SharableSlots::new(slots),
            capacity,
            head: CachePadded::new(AtomicU32::new(0)),
            tail: CachePadded::new(AtomicU32::new(0)),
        }
    }

    /// Returns the number of elements in the buffer.
    ///
    /// This is just a close estimate of the number of elements in the buffer,
    /// since it's possible that there may be readers before any elements have
    /// been written, or concurrent reads or writes happen in between the return
    /// of this call and the time at which the returned value is read.
    pub fn size(&self) -> i32 {
        self.head.load(Ordering::Relaxed) as i32 - self.tail.load(Ordering::Relaxed) as i32
    }

    /// Returns and estimate of whether or not the buffer is empty.
    ///
    /// This is just an estimate since this is ringbuffer is conccurrent and is
    /// therefore not deterministic unless there is a single reader or writer at
    /// the point at which this is called.
    pub fn empty(&self) -> bool {
        self.size() <= 0
    }

    /// Gets the index from the given `value`.
    pub(crate) fn index(&self, value: u32) -> usize {
        (value % self.capacity) as usize
    }

    /// Gets the tag for a given `value`.
    pub(crate) fn tag(&self, value: u32) -> u32 {
        value / self.capacity
    }
}

/// Producer for a concurrent [Ringbuffer]. This struct allows elements to be
/// pushed onto the ringbuffer from multiple threads.
pub struct Producer<T> {
    /// The buffer to produce elements into.
    pub(crate) buffer: Arc<Ringbuffer<T>>,
}

impl<T: Slot> Producer<T>
where
    <T as Slot>::Storage: Sized + Copy,
{
    /// Returns the capacity of the ringbuffer.
    pub fn capacity(&self) -> u32 {
        self.buffer.capacity
    }

    /// Returns an estimate of the size (number of elements) in the buffer.
    pub fn size(&self) -> i32 {
        self.buffer.size()
    }

    /// Returns true if the buffer is empty, and false if it is not.
    pub fn empty(&self) -> bool {
        self.buffer.empty()
    }

    /// Pushes the specified `element` into the ringbuffer for which this producer
    /// can produce elements. This version is slower than calling [Producer::try_push]
    /// as it uses stricter atomics but will alwasy succeed at pushing an element.
    ///
    /// # Errors
    ///
    /// If the buffer is full then this will block until it's possible to push
    /// onto the queue.
    pub fn push(&mut self, element: T::Storage) {
        let head = self.buffer.head.fetch_add(1, Ordering::SeqCst);
        let index = self.buffer.index(head);

        let slot = unsafe { &self.buffer.slots.get()[index] };
        while self.buffer.tag(head) * 2 != slot.acquire_tag() {
            // This should never happend: spinning ..
        }

        self.update_slot_and_release(index, element, head);
    }

    /// Tries to push the specified `element` into the ringbuffer. If the buffer
    /// is full then this will return a [RingbufferError::BufferIsFull] error.
    pub fn try_push(&mut self, element: T::Storage) -> Result<(), RingbufferError> {
        let mut head = self.buffer.head.load(Ordering::Acquire);

        loop {
            let slot_index = self.buffer.index(head);
            let slot = unsafe { &self.buffer.slots.get()[slot_index] };

            // We can try and emplace into the buffer
            if self.buffer.tag(head) * 2 == slot.acquire_tag() {
                // We might race with another thread. If head has not changed
                // and we update it, then we win and can modify the slot. If
                // head has changed, another thread has won the race and we
                // want to load the new value into head and try again.
                match self.buffer.head.compare_exchange(
                    head,
                    head + 1,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.update_slot_and_release(slot_index, element, head);
                        return Ok(());
                    }
                    Err(new_head) => {
                        head = new_head;
                    }
                }
            } else {
                // The tags dont match, which means that another thread must
                // have got the same head index but managed to update the slot
                // before we even got into a race to update the head pointer,
                // so we need to update it now.
                //
                // If they are the same, then the buffer must be full, and we
                // cant push into the buffer.
                let prev_head = head;
                head = self.buffer.head.load(Ordering::Acquire);
                if prev_head == head {
                    return Err(RingbufferError::BufferIsFull);
                }
            }
        }
    }

    /// Updates the data for the slot at the `slot_index` using the data from
    /// the `element`, and then updates the head value using acquire ordering.
    fn update_slot_and_release(&mut self, slot_index: usize, element: T::Storage, head: u32) {
        let tag = self.buffer.tag(head);
        let slot = unsafe { &mut self.buffer.slots.get_mut()[slot_index] };
        unsafe {
            std::ptr::drop_in_place(slot.storage_mut());
        }
        *slot.storage_mut() = element;
        slot.tag().store(tag * 2 + 1, Ordering::Release);
    }
}

/// Consumer for a concurrent [RingBuffer]. This struct allows elements to be
/// popped off of the ringbuffer.
pub struct Consumer<T> {
    /// The buffer to consume elements from.
    pub(crate) buffer: Arc<Ringbuffer<T>>,
}

impl<T: Slot> Consumer<T>
where
    <T as Slot>::Storage: Sized + Copy,
{
    /// Returns the capacity of the ringbuffer.
    pub fn capacity(&self) -> u32 {
        self.buffer.capacity
    }

    /// Returns an estimate of the size (number of elements) in the buffer.
    pub fn size(&self) -> i32 {
        self.buffer.size()
    }

    /// Returns true if the buffer is empty, and false if it is not.
    pub fn empty(&self) -> bool {
        self.buffer.empty()
    }

    /// Pops an element off of the queue into the `element`. This version is
    /// slower than calling [Consumer::try_pop] as it uses stricter atomics but
    /// will always succeed at pushing an element.
    ///
    /// # Errors
    ///
    /// If the buffer is empty then this will block until it's possible to pop
    /// from the queue.
    pub fn pop(&mut self, element: &mut T::Storage) {
        let tail = self.buffer.tail.fetch_add(1, Ordering::SeqCst);
        let index = self.buffer.index(tail);
        let slot = unsafe { &self.buffer.slots.get()[index] };

        while self.buffer.tag(tail) * 2 + 1 != slot.acquire_tag() {
            // Shouldn't happen: spin ...
        }

        self.remove_slot_and_release(index, element, tail)
    }

    /// Tries to pop and element off of the ringbuffer into `element`. If the
    /// buffer is empty then this will return an [RingbufferError::BufferIsEmpty]
    /// error.
    pub fn try_pop(&mut self, element: &mut T::Storage) -> Result<(), RingbufferError> {
        let mut tail = self.buffer.tail.load(Ordering::Acquire);
        loop {
            let slot_index = self.buffer.index(tail);
            let slot = unsafe { &self.buffer.slots.get()[slot_index] };

            // We can try and pop the data from the slot:
            if self.buffer.tag(tail) * 2 + 1 == slot.acquire_tag() {
                // We might be in a race with another thread. If the tail has
                // not changed since the read above, then we can try and take
                // the data from the slot. If the tag has changed then another
                // thread won the race, so we need to reload the tail value to
                // the updated value and try again.
                match self.buffer.tail.compare_exchange(
                    tail,
                    tail + 1,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.remove_slot_and_release(slot_index, element, tail);
                        return Ok(());
                    }
                    Err(new_tail) => {
                        tail = new_tail;
                    }
                }
            } else {
                // The tags dont match, which means that another thread must
                // have got the same tail index but managed to update the slot
                // before we even got into a race to update the tail pointer,
                // so we need to update it now and try again if they don't
                // match (which they shouldn't).
                //
                // If they are the same, then the buffer must be empty, and we
                // cant pop from the buffer.
                let prev_tail = tail;
                tail = self.buffer.tail.load(Ordering::Acquire);
                if prev_tail == tail {
                    return Err(RingbufferError::BufferIsEmpty);
                }
            }
        }
    }

    /// Removes the data from the slot at the given `slot_index`, placing it
    /// into the `element`, and then updates the tail value using release
    /// ordering.
    fn remove_slot_and_release(&mut self, slot_index: usize, element: &mut T::Storage, tail: u32) {
        let tag = self.buffer.tag(tail);
        let slot = unsafe { &mut self.buffer.slots.get_mut()[slot_index] };

        // We would like to do this with placement new, but there isnt that
        // option at present.
        // TODO: Check if ptr::write is more efficient.
        *element = *slot.storage_mut();

        // We might also want to run the destructor here, but we still need to
        // decide if we want to support complex types which require this
        // unsafe {
        //   std::ptr::drop_in_place(slot.storage());
        // }
        slot.tag().store(tag * 2 + 2, Ordering::Release);
    }
}
