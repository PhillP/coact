use std::sync::mpsc::{channel};
use std::sync::mpsc::{Sender, Receiver, RecvError, SendError, TryRecvError};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::{JoinHandle};
use std::marker::PhantomData;

pub struct Pipe<I,O: 'static, T> where T: Fn(I) -> O, O: Clone  {
    out_connectors: Arc<Mutex<Vec<Connector<O>>>>,
    inner_transform: T,
    _marker: PhantomData<I>  
}

unsafe impl<I,O,T> Send for Pipe<I,O,T> where T: Fn(I) -> O, O: Clone {}
unsafe impl<O> Send for Connector<O> {}

pub enum Connector<T> {
    SynchronousConnector {
        pipe: Arc<Mutex<Processor<T>>>
    },
    AsynchronousConnector {
        pipe: Arc<Mutex<Processor<T> + Send>>,
        sender : Arc<Mutex<Sender<T>>>,
        receiver : Arc<Mutex<Receiver<T>>>,
        thread_handle: JoinHandle<()>
    },
    DoNothingConnector
}

impl<I: 'static + Send> Connector<I> {
    pub fn sync_connector(pipe: Arc<Mutex<Processor<I>>>) -> Connector<I> {
        Connector::<I>::SynchronousConnector {
            pipe: pipe
        }
    }
    
    pub fn async_connector(pipe: Arc<Mutex<Processor<I> + Send>>) -> Connector<I> {
        let (tx, rx) = channel::<I>();
        
        let txarc = Arc::new(Mutex::new(tx));
        let rxarc = Arc::new(Mutex::new(rx));
        
        let pipearc_clone = pipe.clone();
        let rxarc_clone = rxarc.clone();
        
        Connector::AsynchronousConnector {
            pipe: pipe,
            sender: txarc,
            receiver: rxarc,
            thread_handle: thread::spawn(move || {
                while(true) {
                    let result = rxarc_clone.lock().unwrap().recv();
        
                    if result.is_ok() {
                        pipearc_clone.lock().unwrap().process(result.ok().unwrap());
                    }
                }                        
            }) 
        }
    }
}

impl<I: 'static + Send, O: 'static, T> Pipe<I, O, T> where T: Fn(I) -> O, O: Clone {
    pub fn new(transform: T) -> Pipe<I, O, T> {
        Pipe {
            out_connectors: Arc::new(Mutex::new(Vec::new())),
            inner_transform: transform,
            _marker: PhantomData
        }
    }
    
    pub fn new_with_connector(transform: T, connector: Connector<O>) -> Pipe<I, O, T> {
        let newPipe = Pipe::<I, O, T>::new(transform);
        
        newPipe.out_connectors.lock().unwrap().push(connector);
        newPipe
    }

    pub fn connect(&mut self, connector: Connector<O>) {
        self.out_connectors.lock().unwrap().push(connector);
    }
}

pub trait Processor<T> {
    fn process(&self, T) -> ();
}

impl<I, O, T> Processor<I> for Pipe<I, O, T> where T: Fn(I) -> O, O: Clone {
    fn process(&self, data: I) -> () {
        let output = (self.inner_transform)(data);
        
        let connectors = self.out_connectors.lock().unwrap();
        
        if (connectors.len() == 1) {
            // only one connector: so it can be given the transformed value without cloning
            connectors.first().unwrap().process(output)
        } else {
            for connector in connectors.iter() {
                // give each connector a clone of the output
                connector.process(output.clone());
            }    
        }
    }
}

impl<T> Processor<T> for Connector<T> {
    fn process(&self, data: T) -> () {
        match *self {
            Connector::DoNothingConnector => (),
            Connector::SynchronousConnector { pipe: ref pipe } => pipe.lock().unwrap().process(data),
            Connector::AsynchronousConnector {
                pipe: ref pipe,
                sender : ref sender,
                receiver : ref receiver,
                thread_handle: ref thread_handle
            }   =>  { sender.lock().unwrap().send(data); },
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
   
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;

    
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
    
    #[test]
    fn pipe() {
        println!("starting");
        // create a Pipe
        let mut double_pipe = Arc::new(Mutex::new(Pipe::new(double)));
        let mut triple_pipe = Arc::new(Mutex::new(Pipe::new(triple)));
        let mut second_double_pipe = Arc::new(Mutex::new(Pipe::new(double)));
        let other_pipe = Arc::new(Mutex::new(Pipe::new(|x:i64| { 
                let result = x*10;
                println!("Result is {}", result);
                result
               })));
               
        let branch_pipe = Arc::new(Mutex::new(Pipe::new(|x:i64| { 
                let result = x*5;
                println!("Result  on branch is {}", result);
                result
               })));
        
        let val:i64 = 50;
        
        let connector1 = Connector::<i64>::sync_connector(triple_pipe.clone());
        let connector2 = Connector::<i64>::async_connector(second_double_pipe.clone());
        let connector3 = Connector::<i64>::sync_connector(other_pipe.clone());
        let branch_connector = Connector::<i64>::async_connector(branch_pipe.clone());
        
        // connect the pipes
        double_pipe.lock().unwrap().connect(connector1);
        triple_pipe.lock().unwrap().connect(connector2);
        second_double_pipe.lock().unwrap().connect(connector3);
        triple_pipe.lock().unwrap().connect(branch_connector);
        
        double_pipe.lock().unwrap().process(val);
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