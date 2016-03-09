use std::sync::mpsc::{channel};
use std::sync::mpsc::{Sender, Receiver, RecvError, SendError, TryRecvError};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

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