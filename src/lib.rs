extern crate core;
extern crate time;

use std::sync::mpsc::{channel};
use std::sync::mpsc::{Sender, Receiver, RecvError, SendError, TryRecvError};
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::thread;
use std::thread::{JoinHandle, sleep};
use std::marker::PhantomData;
use core::ops::Deref;
use time::{Duration, PreciseTime};
use std::time::Duration as StdDuration;

pub struct Pipeline<TIn, TLastIn: 'static, TOut: 'static, T> where T: Fn(TLastIn) -> TOut, TOut: Clone  {
    pipe_in_connector: Arc<RwLock<Connector<TIn>>>,
    pipe_in: Arc<RwLock<Processor<TIn>>>,
    pipe_out: Arc<RwLock<Pipe<TLastIn, TOut, T>>>,
}

impl<TIn: 'static + Send, TLastIn: 'static + Send, TOut: 'static, T> Pipeline<TIn, TLastIn, TOut, T> where TIn: Clone, TLastIn: Clone, TOut: Clone, T: Fn(TLastIn) -> TOut {
    pub fn new(pipe_in: Arc<RwLock<Processor<TIn> + Send + Sync>>, pipe_out: Arc<RwLock<Pipe<TLastIn, TOut, T>>>) -> Pipeline<TIn, TLastIn, TOut, T> {
        let connector = Connector::async_connector(pipe_in.clone(), 1);
        
        Pipeline {
            pipe_in_connector: Arc::new(RwLock::new(connector)),
            pipe_in: pipe_in,
            pipe_out: pipe_out
        }
    }
    
    pub fn flush(&self) {
        self.pipe_in_connector.read().unwrap().process(None);
    }
    
    pub fn flush_and_wait(&self, timeout: Duration) {
      self.flush();
      self.pipe_in.read().unwrap().wait_work_complete(timeout);
    }
    
    pub fn process(&self, data: TIn) {
        self.pipe_in_connector.read().unwrap().process(Some(data));    
    }
}
    
pub struct Pipe<I,O: 'static, T> where T: Fn(I) -> O, O: Clone  {
    out_connectors: Arc<RwLock<Vec<Connector<O>>>>,
    inner_transform: T,
    in_count: Arc<RwLock<i64>>,
    out_count: Arc<RwLock<i64>>,
    _marker: PhantomData<I>  
}

pub struct Filter<I, T> where T: Fn(I) -> bool, I: Clone  {
    out_connectors: Arc<RwLock<Vec<Connector<I>>>>,
    filter_function: T,
    in_count: Arc<RwLock<i64>>,
    out_count: Arc<RwLock<i64>>,
}

pub struct Mapper<I,O: 'static, T> where T: Fn(I) -> O, O: Clone  {
    out_connectors: Arc<RwLock<Vec<Connector<O>>>>,
    inner_transform: T,
    in_count: Arc<RwLock<i64>>,
    out_count: Arc<RwLock<i64>>,
    _marker: PhantomData<I>  
}

pub struct FlatMapper<I,O: 'static, T> where T: Fn(I) -> O, O: Clone  {
    out_connectors: Arc<RwLock<Vec<Connector<O>>>>,
    inner_transform: T,
    in_count: Arc<RwLock<i64>>,
    out_count: Arc<RwLock<i64>>,
    _marker: PhantomData<I>  
}


// Filter

// Mapper

// FlatMapper

// Reducer

// Collector

unsafe impl<I,O,T> Send for Pipe<I,O,T> where T: Fn(I) -> O, O: Clone {}
unsafe impl<I,O,T> Sync for Pipe<I,O,T> where T: Fn(I) -> O, O: Clone {}
unsafe impl<O> Send for Connector<O> {}

pub enum Connector<T> {
    SynchronousConnector {
        pipe: Arc<RwLock<Processor<T>>>,
        in_count: Arc<RwLock<i64>>,
        out_count: Arc<RwLock<i64>>
    },
    AsynchronousConnector {
        pipe: Arc<RwLock<Processor<T> + Send>>,
        sender : Arc<Mutex<Sender<Option<T>>>>,
        receiver : Arc<Mutex<Receiver<Option<T>>>>,
        thread_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
        in_count: Arc<RwLock<i64>>,
        out_count: Arc<RwLock<i64>>
    },
    QueueConnector {
        sender : Arc<Mutex<Sender<T>>>,
        receiver : Arc<Mutex<Receiver<T>>>,
        in_count: Arc<RwLock<i64>>,
        out_count: Arc<RwLock<i64>>
    }
}

impl<I: 'static + Send> Connector<I> {
    pub fn sync_connector(pipe: Arc<RwLock<Processor<I>>>) -> Connector<I> {
        Connector::<I>::SynchronousConnector {
            pipe: pipe,
            in_count: Arc::new(RwLock::new(0)),
            out_count: Arc::new(RwLock::new(0))
        }
    }
    
    pub fn async_connector(pipe: Arc<RwLock<Processor<I> + Send + Sync>>, threads: u8) -> Connector<I> {
        let (tx, rx) = channel::<Option<I>>();
        
        let txarc = Arc::new(Mutex::new(tx));
        let rxarc = Arc::new(Mutex::new(rx));
        
        let handles = Arc::new(Mutex::new(Vec::new()));
        
        let pipearc_clone = pipe.clone();
        let rxarc_clone = rxarc.clone();
            
        let connector = Connector::AsynchronousConnector {
            pipe: pipe,
            sender: txarc,
            receiver: rxarc,
            thread_handles: handles.clone(),
            in_count: Arc::new(RwLock::new(0)),
            out_count: Arc::new(RwLock::new(0))
        };
        
        for _ in 0..threads {
            let pipearc_clone_for_thread = pipearc_clone.clone();
            let rxarc_clone_for_thread = rxarc_clone.clone();
            
            let handle = thread::spawn(move || {
                loop {
                    let result = rxarc_clone_for_thread.lock().unwrap().recv();
                    
                    if result.is_ok() {
                        pipearc_clone_for_thread.read().unwrap().process(result.ok().unwrap());
                    } else {
                        // disconnect the pipe... no longer receiving
                        break; // no longer receiving on the channel
                    }
                }                        
            });
            
            handles.lock().unwrap().push(handle);
        }
        
        connector
    }
    
    pub fn queue_connector() -> Connector<I> {
        let (tx, rx) = channel::<I>();
        
        let txarc = Arc::new(Mutex::new(tx));
        let rxarc = Arc::new(Mutex::new(rx));
        
        Connector::QueueConnector {
            sender: txarc,
            receiver: rxarc,
            in_count: Arc::new(RwLock::new(0)),
            out_count: Arc::new(RwLock::new(0))
        }
    }
    
    /// Receive a message
    ///
    /// If there is no message on the channel this call will block until there is one 
    ///
    /// Returns a Result<I, RecvError>
    ///
    /// # Examples
    ///
    /// ```text
    /// let sr = channel_pair.second_sender_receiver();
    /// // . . .
    /// let result = sr.receive();
    /// if result.is_ok() {
    ///     let val = result.ok().unwrap();
    ///     // do something
    /// }
    /// ```
    pub fn receive(&self) -> Result<I, RecvError> {
        match *self {
            Connector::SynchronousConnector { 
                pipe: _, 
                in_count: _,
                out_count: _ 
            } => { panic!("SynchronousConnector is incapable of receive()"); },
            Connector::AsynchronousConnector {
                pipe: _,
                sender: _,
                receiver: _,
                thread_handles: _,
                in_count: _,
                out_count: _
            } =>  { panic!("AsynchronousConnector is incapable of receive()"); },
            Connector::QueueConnector {
                sender: _,
                ref receiver,
                in_count: _,
                ref out_count
            } =>  { 
                let result = receiver.lock().unwrap().recv();
                
                if result.is_ok() {
                    let mut out_count = out_count.write().unwrap();
                    *out_count += 1;
                }
                
                result 
            }
        }
    }
    
    /// Try to receive a message if one is available
    ///
    /// If there is no message on the channel this call will return a result containing a `TryRecvError` 
    ///
    /// Returns a Result<I, TryRecvError>
    ///
    /// # Examples
    ///
    /// ```text
    /// let sr = channel_pair.second_sender_receiver();
    /// // . . .
    /// let result = sr.try_receive();
    /// if result.is_ok() {
    ///     let val = result.ok().unwrap();
    ///     // do something
    /// }
    /// ```
    pub fn try_receive(&self) -> Result<I, TryRecvError> {
        match *self {
            Connector::SynchronousConnector { 
                pipe: _, 
                in_count: _,
                out_count: _ 
            } => { panic!("SynchronousConnector is incapable of try_receive()"); },
            Connector::AsynchronousConnector {
                pipe: _,
                sender: _,
                receiver: _,
                thread_handles: _,
                in_count: _,
                out_count: _
            } =>  { panic!("AsynchronousConnector is incapable of try_receive()"); },
            Connector::QueueConnector {
                sender: _,
                ref receiver,
                in_count: _,
                ref out_count
            } =>  { 
                let result = receiver.lock().unwrap().try_recv();
 
                if result.is_ok() {
                    let mut out_count = out_count.write().unwrap();
                    *out_count += 1;
                }
 
                result 
            }
        }
    }
}

impl<I: 'static + Send, O: 'static, T> Pipe<I, O, T> where T: Fn(I) -> O, O: Clone {
    pub fn new(transform: T) -> Pipe<I, O, T> {
        Pipe {
            out_connectors: Arc::new(RwLock::new(Vec::new())),
            inner_transform: transform,
            in_count: Arc::new(RwLock::new(0)),
            out_count: Arc::new(RwLock::new(0)),
            _marker: PhantomData
        }
    }
    
    pub fn new_with_connector(transform: T, connector: Connector<O>) -> Pipe<I, O, T> {
        let new_pipe = Pipe::<I, O, T>::new(transform);
        
        new_pipe.out_connectors.write().unwrap().push(connector);
        new_pipe
    }
    
    pub fn connect(&mut self, connector: Connector<O>) {
        self.out_connectors.write().unwrap().push(connector);
    }
}

pub trait Processor<T> {
    fn process(&self, Option<T>) -> ();
    
    fn get_processed_count(&self) -> i64;
    
    fn has_work_remaining(&self) -> bool;
    
    fn wait_work_complete(&self, timeout: Duration) -> bool {
        let mut has_timed_out = false;
        let start = PreciseTime::now();
        
        while !has_timed_out && self.has_work_remaining() {
            let duration_since_start = start.to(PreciseTime::now());
            
            if duration_since_start > timeout {
                has_timed_out = true;
            } else {
                sleep(StdDuration::from_millis(500));
            }
        } 
    
        !has_timed_out
    }
}

impl<I, O, T> Processor<I> for Pipe<I, O, T> where T: Fn(I) -> O, O: Clone {
    
    fn process(&self, data: Option<I>) -> () {

        let output = {
            // only perform the transformation is Some(data) was provided
            match data {
                None => None,
                Some(inner_data) => Some((self.inner_transform)(inner_data)),
            }
        };
        
        // count inputs
        let mut in_count = self.in_count.write().unwrap();
        *in_count += 1;
        
        let connectors = self.out_connectors.read().unwrap();
        
        if connectors.len() == 1 {
            // only one connector: so it can be given the transformed value without cloning
            connectors.first().unwrap().process(output)
        } else {
            for connector in connectors.iter() {
                // give each connector a clone of the output
                connector.process(output.clone());
            }    
        }
        
        // count outputs
        let mut out_count = self.out_count.write().unwrap();
        *out_count += 1;
    }
    
    fn has_work_remaining(&self) -> bool {
        let mut has_work = false;
        
        let in_count = *(self.in_count.read().unwrap().deref());
        let out_count = *(self.out_count.read().unwrap().deref());
        
        if in_count > out_count {
            has_work = true;
        } else {
          let connectors = self.out_connectors.read().unwrap();
        
          for connector in connectors.iter() {
            if connector.has_work_remaining() {
                has_work = true;
                break;        
            } 
            
            if out_count > connector.get_processed_count() {
                has_work = true;
                break;
            }
          }    
        }
        
        has_work
    }
    
    fn get_processed_count(&self) -> i64 {
        *(self.out_count.read().unwrap().deref())
    }    
}

impl<T> Processor<T> for Connector<T> {
    fn process(&self, data: Option<T>) -> () {
        match *self {
            Connector::SynchronousConnector { 
                ref pipe,
                ref in_count, 
                ref out_count 
                } => {
                    let mut in_count = in_count.write().unwrap();
                    *in_count += 1;
         
                    pipe.read().unwrap().process(data);
                    
                    let mut out_count = out_count.write().unwrap();
                    *out_count += 1;
                },
            Connector::AsynchronousConnector {
                pipe: _,
                ref sender,
                receiver: _,
                thread_handles: _,
                ref in_count, 
                ref out_count 
            } =>  { 
                let mut in_count = in_count.write().unwrap();
                *in_count += 1;
                
                sender.lock().unwrap().send(data);
                
                let mut out_count = out_count.write().unwrap();
                *out_count += 1;
            },
            Connector::QueueConnector {
                ref sender,
                receiver: _,
                ref in_count, 
                ref out_count 
            } =>  { 
                let mut in_count = in_count.write().unwrap();
                *in_count += 1;
                
                match data {
                    None => Ok(()),
                    Some(inner_data) => sender.lock().unwrap().send(inner_data), 
                };
            }
        }
    }
    
    fn has_work_remaining(&self) -> bool {
        match *self {
            Connector::SynchronousConnector { 
                ref pipe,
                ref in_count, 
                ref out_count 
                } => {
                    let mut has_work = false;
                    let in_count_unwrap = *(in_count.read().unwrap().deref());
                    let out_count_unwrap = *(out_count.read().unwrap().deref());
            
                    if in_count_unwrap > out_count_unwrap {
                       has_work = true;
                    } else {
                       if pipe.read().unwrap().has_work_remaining() {
                           has_work = true;
                       } else {
                            let pipe_processed_count = pipe.read().unwrap().get_processed_count();
                            if out_count_unwrap > pipe_processed_count {
                                has_work = true;
                            }
                       }
                    }
                    
                    has_work
                },
            Connector::AsynchronousConnector {
                ref pipe,
                sender: _,
                receiver: _,
                thread_handles: _,
                ref in_count, 
                ref out_count 
            } =>  { 
                let mut has_work = false;
                let in_count_unwrap = *(in_count.read().unwrap().deref());
                let out_count_unwrap = *(out_count.read().unwrap().deref());
            
                if in_count_unwrap > out_count_unwrap {
                    has_work = true;
                } else {
                    if pipe.read().unwrap().has_work_remaining() {
                        has_work = true;
                    } else {
                        let pipe_processed_count = pipe.read().unwrap().get_processed_count();
                        if out_count_unwrap > pipe_processed_count {
                            has_work = true;
                        }
                    }
                }
                
                has_work
            },
            Connector::QueueConnector {
                sender: _,
                receiver: _,
                ref in_count, 
                ref out_count 
            } =>  { 
                let in_count_unwrap = *(in_count.read().unwrap().deref());
                let out_count_unwrap = *(out_count.read().unwrap().deref());
                
                return in_count_unwrap > out_count_unwrap;
            }
        }
    }
    
    fn get_processed_count(&self) -> i64 {
        match *self {
            Connector::SynchronousConnector { 
                pipe: _,
                in_count: _, 
                ref out_count 
                } => {
                    let count:i64 = *(out_count.read().unwrap().deref());
                count
                },
            Connector::AsynchronousConnector {
                pipe: _,
                sender: _,
                receiver: _,
                thread_handles: _,
                in_count: _, 
                ref out_count 
            } =>  { 
                let count:i64 = *(out_count.read().unwrap().deref());
                count
            },
            Connector::QueueConnector {
                sender: _,
                receiver: _,
                in_count: _, 
                ref out_count 
            } =>  { 
                let count:i64 = *(out_count.read().unwrap().deref());
                count
            }
        }
    }
}

/// Represents a pair of channels used for bi-directional communication.
/// T1 represents the type of data sent in one direction
/// T2 represents the type of data sent in the reverse direction
pub struct ChannelPair<T1, T2> {
    sender1 : Arc<Mutex<Sender<T1>>>,
    receiver1 : Arc<Mutex<Receiver<T1>>>,
    sender2 : Arc<Mutex<Sender<T2>>>,
    receiver2 : Arc<Mutex<Receiver<T2>>>
}

/// A sender and receiver pair that through which a thread can send messages and receive messages respectively
/// The sender and receiver are taken from opposite channels of a ChannelPair
pub struct SenderReceiverPair<T1, T2> {
    sender : Arc<Mutex<Sender<T1>>>,
    receiver : Arc<Mutex<Receiver<T2>>>,
    send_count: i64,
    receive_count: i64
}

/// An enumeration type representing either a send error or a receive error
pub struct SendOrReceiveError<T>
{
    send_error: Option<SendError<T>>,
    receive_error: Option<RecvError>
}

/// Implementation of a ChannelPair
impl<T1,T2> ChannelPair<T1, T2> {
    /// Constructs a new `ChannelPair<T1, T2>`.
    ///
    /// Each channel within the pair is constructed with a buffer size of 0.
    /// A buffer size of 0 means that a sender will be blocked from sending additional messages
    /// until any previously sent message has been received.
    ///
    /// # Examples
    ///
    /// ```text
    /// use coact::ChannelPair;
    ///
    /// let channel_pair = ChannelPair::new();
    /// ```
    pub fn new() -> ChannelPair<T1, T2> {
        ChannelPair::new_with_buffer_size(0, 0)
    }
    
    /// Constructs a new `ChannelPair<T1, T2>` with buffer sizes specified for each channel.
    ///
    /// The buffer size indicates how many messages can exist on the channel before a sender is blocked from adding additional messages 
    /// Each channel in the pair can have a different buffer size
    ///
    /// # Examples
    ///
    /// ```text
    /// use coact::ChannelPair;
    ///
    /// let channel_pair = ChannelPair::new_with_buffer_size(5,3);
    /// ```
    pub fn new_with_buffer_size(buffer_size1 : usize, buffer_size2: usize) -> ChannelPair<T1, T2> {
        
        let (tx, rx) = channel::<T1>();
        let (tx2, rx2) = channel::<T2>();
        
        ChannelPair {
            sender1 : Arc::new(Mutex::new(tx)),
            receiver1 : Arc::new(Mutex::new(rx)),
            sender2 : Arc::new(Mutex::new(tx2)),
            receiver2 : Arc::new(Mutex::new(rx2))
        }
    }
    
    /// Constructs a Sender Receiver pair representing one end of the bi-directional communication
    ///
    /// # Examples
    ///
    /// ```text
    /// use coact::ChannelPair;
    ///
    /// let channel_pair : ChannelPair<i64,i32> = ChannelPair::new();
    /// let sr = channel_pair.first_sender_receiver();
    /// ```
    pub fn first_sender_receiver(&self) -> Box<SenderReceiverPair<T1, T2>> {
        Box::new(SenderReceiverPair {
            sender: self.sender1.clone(),
            receiver: self.receiver2.clone(),
            send_count: 0,
            receive_count: 0
        })
    }
    
    /// Constructs a Sender Receiver pair representing the reverse end of the bi-directional communication
    ///
    /// # Examples
    ///
    /// ```text
    /// use coact::ChannelPair;
    ///
    /// let channel_pair : ChannelPair<i64,i32> = ChannelPair::new();
    /// let sr = channel_pair.second_sender_receiver();
    /// ```
    pub fn second_sender_receiver(&self) -> Box<SenderReceiverPair<T2, T1>> {
        Box::new(SenderReceiverPair {
            sender: self.sender2.clone(),
            receiver: self.receiver1.clone(),
            send_count: 0,
            receive_count: 0
        })
    }
}

/// Implementation of `SenderReceiverPair<T1, T2>`
impl<T1,T2> SenderReceiverPair<T1, T2> {
    
    /// Sends a message without waiting for a response
    ///
    /// Even though this call can return before a response 
    /// is received on the receiver, this call may still block if the buffer associated with the channel is full 
    ///
    /// Returns a Result<(), SendError<T1>>
    ///
    /// # Examples
    ///
    /// ```text
    /// let sr = channel_pair.first_sender_receiver();
    /// let result = sr.send(50);
    /// if result.is_err() {
    ///        // do something
    /// }
    /// ```
    pub fn send(&mut self, payload: T1) -> Result<(), SendError<T1>> {
        let result = self.sender.lock().unwrap().send(payload);
        
        if result.is_ok() {
            self.send_count = self.send_count + 1;
        }
        
        result
    }
    
    /// Receive a message
    ///
    /// If there is no message on the channel this call will block until there is one 
    ///
    /// Returns a Result<T2, RecvError>
    ///
    /// # Examples
    ///
    /// ```text
    /// let sr = channel_pair.second_sender_receiver();
    /// // . . .
    /// let result = sr.receive();
    /// if result.is_ok() {
    ///     let val = result.ok().unwrap();
    ///     // do something
    /// }
    /// ```
    pub fn receive(&mut self) -> Result<T2, RecvError> {
        let result = self.receiver.lock().unwrap().recv();
        
        if result.is_ok() {
            self.receive_count = self.receive_count + 1;
        }
        
        result
    }
    
    /// Try to receive a message if one is available
    ///
    /// If there is no message on the channel this call will return a result containing a `TryRecvError` 
    ///
    /// Returns a Result<T2, TryRecvError>
    ///
    /// # Examples
    ///
    /// ```text
    /// let sr = channel_pair.second_sender_receiver();
    /// // . . .
    /// let result = sr.try_receive();
    /// if result.is_ok() {
    ///     let val = result.ok().unwrap();
    ///     // do something
    /// }
    /// ```
    pub fn try_receive(&mut self) -> Result<T2, TryRecvError> {
        let result = self.receiver.lock().unwrap().try_recv();
        
        if result.is_ok() {
            self.receive_count = self.receive_count + 1;
        }
        
        result
    }
    
    /// Send a message and wait for a response
    ///
    /// This call will block until a response is recieved 
    ///
    /// Returns a Result<T2, SendOrReceiveError<T1>>
    ///
    /// # Examples
    ///
    /// ```text
    /// let sr = channel_pair.second_sender_receiver();
    /// // . . .
    /// let result = sr.send_and_receive(56);
    /// if result.is_ok() {
    ///     let val = result.ok().unwrap();
    ///     // do something
    /// }
    /// ```
    pub fn send_and_receive(&mut self, payload: T1) -> Result<T2, SendOrReceiveError<T1>> {
        let send_result = self.send(payload);
        
        if !send_result.is_ok() {
            return Err(SendOrReceiveError { send_error: send_result.err(), receive_error: None });
        }
        
        let receive_result = self.receive();
        
        if !receive_result.is_ok() {
            return Err(SendOrReceiveError { send_error: None, receive_error: receive_result.err() });
        }
        
        Ok(receive_result.ok().unwrap())
    }
}

/// Indicates the `SenderReceiverPair<T1, T2>` is safe for Sync (multi-threaded) operations
unsafe impl<T1, T2> Sync for SenderReceiverPair<T1, T2>  {}

macro_rules! connector {
    
    ( $threads:expr, sync $nexty:ty => $next:ident ) => {
        {
            Connector::<$nexty>::sync_connector($next.clone())
        }
    };
    
    ( $threads:expr, async $nexty:ty => $next:ident ) => {
        {
            Connector::<$nexty>::async_connector($next.clone(), $threads)
        }
    };
    
}

macro_rules! connect {
    ( $threads:expr, $last:ident, $($mode:tt $nexty:ty => $next:expr ),* ) => {
        {
            $(
                let next = Arc::new(RwLock::new(Pipe::new($next)));
            
                let connector = connector!($threads, $mode $nexty => next);
                
                $last.write().unwrap().connect(connector);
                let $last = next.clone();
            )*
        }
    };
}

macro_rules! pipeline {
    ( async_connector_threads: $threads:expr, $headty:ty => $head:expr, $($mode:tt $nexty:ty => $next:expr),* => $outty:ty) => {
        {
            let pipe = Arc::new(RwLock::new(Pipe::new($head)));
            let last = pipe.clone();
            
            connect!($threads, last, $($mode $nexty => $next),*);
            
            Pipeline::new(pipe, last.clone())
        }
    };
        
    ( $headty:ty => $head:expr, $($mode:tt $nexty:ty => $next:expr),* => $outty:ty) => {
        {
            pipeline!(async_connector_threads: 4, $headty => $head, $($mode $nexty => $next),* => $outty:ty)
        }
    };
}
   
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, RwLock};
    use std::thread;
    use std::thread::{sleep};
    use time::{Duration};
    use std::time::Duration as StdDuration;
    
    
    fn double(i:i64) -> i64 {
        println!("double");
        format!("Value before double is: {}",i);
        i*2
    }
    fn triple(i:i64) -> i64 {
        println!("triple");
        format!("Value before triple is: {}",i);
        i*3
    }
    
    #[derive(Clone)]
    struct TestResult {
        depth: i32,
        result: i64
    }
    
    #[test]
    fn pipe_macro() {
        let pipeline = pipeline!(async_connector_threads: 8,
                            i64 => |x:i64|{x*2},
                            sync i64 => |x:i64|{x*10},
                            sync i64 => |x:i64|{x+2},
                            async i64 => |x:i64| { 
                                let result = x*10;
                                println!("Result is {}", result);
                                TestResult {
                                    depth: 0,
                                    result: result
                                }
                            }
                            => TestResult
                            );
                          
       pipeline.process(5);
       println!("Flush and wait....");
                                
       pipeline.flush_and_wait(Duration::milliseconds(10000));
       println!("Done....");
    }
    
    #[test]
    fn pipe() {
        println!("starting");
        // create a Pipe
        let double_pipe = Arc::new(RwLock::new(Pipe::new(double)));
        let triple_pipe = Arc::new(RwLock::new(Pipe::new(triple)));
        let second_double_pipe = Arc::new(RwLock::new(Pipe::new(double)));
        
        let other_pipe = Arc::new(RwLock::new(Pipe::new(|x:i64| { 
                let result = x*10;
                println!("Result is {}", result);
                sleep(StdDuration::from_millis(2000));
                TestResult {
                    depth: 0,
                    result: result
                }
               })));
               
        let branch_pipe = Arc::new(RwLock::new(Pipe::new(|x:i64| { 
                let result = x*5;
                println!("Result  on branch is {}", result);
                result
               })));
        
        let val:i64 = 50;
        
        let connector1 = Connector::<i64>::sync_connector(triple_pipe.clone());
        let connector2 = Connector::<i64>::async_connector(second_double_pipe.clone(), 4);
        let connector3 = Connector::<i64>::sync_connector(other_pipe.clone());
        let branch_connector = Connector::<i64>::async_connector(branch_pipe.clone(), 2);
        let queue_connector = Connector::<TestResult>::queue_connector();
        
        // connect the pipes
        double_pipe.write().unwrap().connect(connector1);
        triple_pipe.write().unwrap().connect(connector2);
        second_double_pipe.write().unwrap().connect(connector3);
        //triple_pipe.write().unwrap().connect(branch_connector);
        other_pipe.write().unwrap().connect(queue_connector);
        
        double_pipe.write().unwrap().process(Some(val));
        double_pipe.write().unwrap().process(Some(50));
        double_pipe.write().unwrap().process(Some(36));
        
        for i in 0..3 {
            let last_pipe = other_pipe.read().unwrap();
            let last_connectors = last_pipe.out_connectors.read().unwrap();
            let last_connector = last_connectors.first().unwrap();
            let result = last_connector.receive();
            if result.is_ok() {
             println!("In final loop {}.. result is {}", i, result.ok().unwrap().result);
            }
        }
        
        let completed = double_pipe.write().unwrap().wait_work_complete(Duration::milliseconds(10000));
        
        println!("Work completed {}", completed);
        
    }
    
    #[test]
    fn coordinate_via_bidirectional() {
       
        // create a ChannelPair
        let chan_pair : Arc<ChannelPair<i64,i64>> = Arc::new(ChannelPair::new_with_buffer_size(0, 0));

        // get one end of the bi-direction communication
        let mut controller_receive_responder = chan_pair.first_sender_receiver();
        
        // get the reverse end of the bi-direction communication
        let mut worker_sender_waiter = chan_pair.second_sender_receiver();
        
        // spawn a thread to perform work in coordination with the main thread
        {
            thread::spawn(|| {
                let mut sw = worker_sender_waiter;
                let mut x:i64 = 0;
                // send a value to the main thread and get the response
                let rpayload = sw.send_and_receive(x).ok().unwrap();
                // increment the value returned from the main thread
                x =  rpayload + 5;
                // send the new value to the main thread and wait for a response
                let rpayload = sw.send_and_receive(x).ok().unwrap();
                // increment the value returned from the main thread
                x =  rpayload + 3;
                // send the new value to the main thread and wait for a response
                let _ = sw.send_and_receive(x).ok().unwrap();
            });
        }
        // receive a value from the worker thread
        let mut mpayload = controller_receive_responder.receive().ok().unwrap();
        // send an incremented value back
        controller_receive_responder.send(mpayload + 3);
        // receive a value from the worker thread
        mpayload = controller_receive_responder.receive().ok().unwrap();
        // send an incremented value back
        controller_receive_responder.send(mpayload + 3);
        // receive a value from the worker thread
        mpayload = controller_receive_responder.receive().ok().unwrap();
        // send an incremented value back
        controller_receive_responder.send(mpayload + 3);
        
        // check that the threads have worked together to produce the expected result
        assert_eq!(14, mpayload);     
    }
}