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
use std::collections::HashMap;
use core::hash::Hash;

pub struct Pipeline<TIn, TLastIn: 'static, TOut: 'static> where TOut: Clone  {
    pipe_in_connector: Arc<RwLock<Connector<TIn>>>,
    pipe_in: Arc<RwLock<Processor<TIn>>>,
    pipe_out: Arc<RwLock<Pipe<TLastIn, TOut>>>,
}

impl<TIn: 'static + Send, TLastIn: 'static + Send, TOut: 'static> Pipeline<TIn, TLastIn, TOut> where TIn: Clone, TLastIn: Clone, TOut: Clone {
    pub fn new(pipe_in: Arc<RwLock<Processor<TIn> + Send + Sync>>, pipe_out: Arc<RwLock<Pipe<TLastIn, TOut>>>) -> Pipeline<TIn, TLastIn, TOut> {
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
    
    pub fn flush_and_wait(&self, timeout: StdDuration) -> bool {
      let current_batch_count = {
          let pipe_out = self.pipe_out.read().unwrap();
          let count = *(pipe_out.processing_state.complete_batch_count.lock().unwrap().deref());
          
          count   
      };
      
      self.flush();
      
      let pipeout =  self.pipe_out.read().unwrap();
      pipeout.processing_state.wait_complete_batch_count(current_batch_count + 1, timeout)
    }
    
    pub fn process(&self, data: TIn) {
        self.pipe_in_connector.read().unwrap().process(Some(data));    
    }
}

pub struct ProcessingState  {
    is_batch_ending_check_lock: Mutex<i8>,
    is_batch_ending: Mutex<bool>,
    state_change_condvar: Condvar,
    complete_batch_count: Mutex<i64>,
    batch_completed_condvar: Condvar,
    in_count: RwLock<i64>,
    out_count: RwLock<i64>,
}

#[derive(Clone)]
pub struct GroupedData<T,GK> where T:Clone, GK:Clone {
    grouping_key: GK,
    data: T
}

unsafe impl<T,GK> Send for GroupedData<T,GK> {}

impl<T: 'static,GK> GroupedData<T,GK> where T:Clone, GK:Clone {
    pub fn new(group_key: GK, data: T) -> GroupedData<T,GK> {
        GroupedData {
            grouping_key: group_key,
            data: data
        }
    }
}

#[derive(Clone)]
pub struct GroupedIterator<T,GK> where T:Clone, GK:Clone {
    grouping_key: GK,
    data: Arc<ItemCache<T>>,
    iterator: Arc<ItemIterator<T>>
}

unsafe impl<T,GK> Send for GroupedIterator<T,GK> {}

impl<T: 'static,GK> GroupedIterator<T,GK> where T:Clone, GK:Clone {
    pub fn new_with_first(grouping_key: GK, first: T) -> GroupedIterator<T,GK> {
        let item_cache = Arc::new(ItemCache::new());
        
        let grouped_iterator = GroupedIterator {
            grouping_key: grouping_key,
            data: item_cache.clone(),
            iterator: Arc::new(ItemIterator::new(item_cache.clone()))
        };
        
        grouped_iterator.data.push(Some(first));
        
        grouped_iterator
    }
}

pub struct Pipe<I,O> where O: Clone {
    out_connectors: Arc<RwLock<Vec<Connector<O>>>>,
    pipe_processor: Arc<PipeProcessor<I,O>>,
    processing_state: Arc<ProcessingState>,
    _marker: PhantomData<I>
}

/*
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

*/

unsafe impl<I,O> Send for Pipe<I,O> where O: Clone {}
unsafe impl<I,O> Sync for Pipe<I,O> where O: Clone {}
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

impl ProcessingState {
    pub fn new() -> ProcessingState {
        ProcessingState {
            is_batch_ending_check_lock: Mutex::new(0),
            is_batch_ending: Mutex::new(false),
            state_change_condvar: Condvar::new(),
            in_count: RwLock::new(0),
            out_count: RwLock::new(0),
            complete_batch_count: Mutex::new(0),
            batch_completed_condvar: Condvar::new()
        }
    }
    
    pub fn wait_if_batch_ending(&self) {
        let mut batch_ending = self.is_batch_ending.lock().unwrap();
        while *batch_ending {
            batch_ending = self.state_change_condvar.wait(batch_ending).unwrap();
        }
    }
    
    pub fn set_batch_ending(&self) {
        let mut batch_ending = self.is_batch_ending.lock().unwrap();
        *batch_ending = true;
        self.state_change_condvar.notify_all();
    }
    
    pub fn set_batch_ended(&self) {
        let mut batch_ending = self.is_batch_ending.lock().unwrap();
        *batch_ending = false;
        
        self.state_change_condvar.notify_all();
        
        let mut complete_batch_count = self.complete_batch_count.lock().unwrap();
        *complete_batch_count += 1;
        
        self.batch_completed_condvar.notify_all();
    }
    
    pub fn wait_complete_batch_count(&self, wait_count: i64, timeout: StdDuration) -> bool {
        let mut complete_batch_count = self.complete_batch_count.lock().unwrap();
        let mut timed_out = false;
        println!("Complete batch count {}",*complete_batch_count);
        while *complete_batch_count < wait_count && !timed_out {
            let (cbc, wait_timeout) = self.batch_completed_condvar.wait_timeout(complete_batch_count, timeout).unwrap();
             
            if wait_timeout.timed_out() {
                 timed_out = true;
                 break;    
            }
            
            complete_batch_count = cbc;
        }
        
        timed_out
    }
}

impl<I: 'static + Send, O: 'static> Pipe<I, O> where I: Clone, O: Clone {
    pub fn new<Transform: 'static>(transform: Transform) -> Pipe<I, O> where Transform: Fn(I) -> O {
        Pipe {
            out_connectors: Arc::new(RwLock::new(Vec::new())),
            pipe_processor: Arc::new(TransformPipeProcessor::<I,O,Transform>::new(transform)),
            processing_state: Arc::new(ProcessingState::new()),
            _marker: PhantomData
        }
    }
    
    pub fn filter<Filter: 'static>(filter_function: Filter) -> Pipe<I, I> where Filter: Fn(I) -> bool {
        Pipe::<I,I> {
            out_connectors: Arc::new(RwLock::new(Vec::new())),
            pipe_processor: Arc::new(FilterPipeProcessor::<I,Filter>::new(filter_function)),
            processing_state: Arc::new(ProcessingState::new()),
            _marker: PhantomData
        }
    }
    
    pub fn stub() -> Pipe<I, O> {
        Pipe::<I,O> {
            out_connectors: Arc::new(RwLock::new(Vec::new())),
            pipe_processor: Arc::new(StubPipeProcessor::<I,O>::new()),
            processing_state: Arc::new(ProcessingState::new()),
            _marker: PhantomData
        }
    }
    
    pub fn iter<IterFunction, TIterator>(iter_function: IterFunction) -> Arc<RwLock<Pipe<I, O>>>
        where
            TIterator: Iterator<Item=O>,
            IterFunction: Fn(ItemIterator<I>) -> TIterator, IterFunction: 'static + Send,
            I: Sync {
        
            let pipe = Arc::new(RwLock::new(Pipe::stub()));
            
            let pipe_processor = Arc::new(IteratorPipeProcessor::new(iter_function, pipe.clone()));
            pipe.write().unwrap().set_pipe_processor(pipe_processor);
            
            pipe
        }

    pub fn consumer<ConsumerFunction>(consumer_function: ConsumerFunction) -> Arc<RwLock<Pipe<I, O>>>
        where
            ConsumerFunction: Fn(ItemIterator<I>) -> O, ConsumerFunction: 'static + Send,
            I: Sync {
        
            let pipe = Arc::new(RwLock::new(Pipe::stub()));
            
            let pipe_processor = Arc::new(ConsumerPipeProcessor::new(consumer_function, pipe.clone()));
            pipe.write().unwrap().set_pipe_processor(pipe_processor);
            
            pipe
        }
        
    pub fn set_pipe_processor(&mut self, pipe_processor:Arc<PipeProcessor<I,O>>) {
        self.pipe_processor = pipe_processor;
    }
    
    pub fn flatmap<FlatMapFunction: 'static>(flatmap_function: FlatMapFunction) -> Pipe<I, O> where FlatMapFunction: Fn(I) -> Box<Iterator<Item=O>> {
        Pipe::<I,O> {
            out_connectors: Arc::new(RwLock::new(Vec::new())),
            pipe_processor: Arc::new(FlatMapPipeProcessor::<I,O,FlatMapFunction>::new(flatmap_function)),
            processing_state: Arc::new(ProcessingState::new()),
            _marker: PhantomData
        }
    }
    
    pub fn new_with_connector<Transform: 'static>(transform: Transform, connector: Connector<O>) -> Pipe<I, O> where Transform: Fn(I) -> O {
        let new_pipe = Pipe::<I, O>::new(transform);
        
        new_pipe.out_connectors.write().unwrap().push(connector);
        new_pipe
    }
    
    pub fn connect(&mut self, connector: Connector<O>) {
        self.out_connectors.write().unwrap().push(connector);
    }
    
    fn process_in(&self, data: Option<I>) -> Option<I> {  
         let batch_ending_signifier = {
            
            // hold the lock check mutex... onlt one thread allowed here at a time
            let lock = self.processing_state.is_batch_ending_check_lock.lock();
                        
            if data.is_none() {
                // if a batch is already ending.. wait here until the previous batch has ended
                self.processing_state.wait_if_batch_ending();
                
                // then set batch ending
                self.processing_state.set_batch_ending();
                
                true // this is the end of a batch
            } else {
                false
            }
        };
        
        if !batch_ending_signifier {
            // normal data must wait if the batch is ending
            self.processing_state.wait_if_batch_ending();
        }
        
        data
    } 

    fn process_out(&self, data: Option<O>) -> () {  
        let connectors = self.out_connectors.read().unwrap();
        let is_batch_end_signifier = data.is_none();
        
        if connectors.len() == 1 {
            // only one connector: so it can be given the transformed value without cloning
            connectors.first().unwrap().process(data)
        } else {
            for connector in connectors.iter() {
                // give each connector a clone of the output
                connector.process(data.clone());
            }    
        }
        
        if is_batch_end_signifier {
            self.processing_state.set_batch_ended();
        } else {
            // count normal outputs only
            let mut out_count = self.processing_state.out_count.write().unwrap();
            *out_count += 1;
        }
    }
}


pub trait PipeProcessor<TIn, TOut> {
    fn process_with_callback(&self, input: Option<TIn>, pipe: &Pipe<TIn,TOut>) -> ();   
}

pub struct TransformPipeProcessor<TIn, TOut, Transform> 
    where   
            Transform: Fn(TIn) -> TOut, 
            TOut: Clone  {
    inner_transform: Transform,
    _marker1: PhantomData<TIn>,
    _marker2: PhantomData<TOut>
}

impl<TIn: 'static + Send, TOut: 'static, Transform: 'static> TransformPipeProcessor<TIn, TOut, Transform>
    where
            Transform: Fn(TIn) -> TOut, 
            TOut: Clone {

    pub fn new(transform: Transform) -> TransformPipeProcessor<TIn, TOut, Transform> {
        TransformPipeProcessor {
            inner_transform: transform,
            _marker1: PhantomData,
            _marker2: PhantomData
        }
    }
}

impl<TIn: 'static + Send, TOut: 'static, Transform> PipeProcessor<TIn, TOut> for TransformPipeProcessor<TIn, TOut, Transform> 
    where
        Transform: Fn(TIn) -> TOut,
        TIn: Clone, 
        TOut: Clone {
 
    fn process_with_callback(&self, input: Option<TIn>, pipe: &Pipe<TIn,TOut>) -> () {
        
        let yield_output = |o:TOut| { pipe.process_out(Some(o)); };
        
        match input {
            None => {
                pipe.process_out(None);
            },
            Some(inner_data) => {
                let transformed_output = (self.inner_transform)(inner_data);
                yield_output(transformed_output);            
            },
        };
    }
}

pub struct StubPipeProcessor<TIn, TOut> {
    _marker1: PhantomData<TIn>,
    _marker2: PhantomData<TOut>
}

impl<TIn: 'static + Send, TOut: 'static> StubPipeProcessor<TIn, TOut>
    where   TIn: Clone  {
        
    pub fn new() -> StubPipeProcessor<TIn, TOut> {
        StubPipeProcessor {
            _marker1: PhantomData,
            _marker2: PhantomData
        }
    }
}

impl<TIn: 'static + Send, TOut: 'static> PipeProcessor<TIn, TOut> for StubPipeProcessor<TIn, TOut>  
    where   TIn: Clone  {
 
    fn process_with_callback(&self, input: Option<TIn>, pipe: &Pipe<TIn,TOut>) -> () {
        panic!("StubPipeProcessor should be used only as a placeholder during construction but was used for processing.");
    }
}

pub struct FilterPipeProcessor<TIn, Filter> 
    where   Filter: Fn(TIn) -> bool, TIn: Clone  {
    inner_filter: Filter,
    _marker1: PhantomData<TIn>
}

impl<TIn: 'static + Send, Filter: 'static> FilterPipeProcessor<TIn, Filter>
    where   Filter: Fn(TIn) -> bool, TIn: Clone {

    pub fn new(filter: Filter) -> FilterPipeProcessor<TIn, Filter> {
        FilterPipeProcessor {
            inner_filter: filter,
            _marker1: PhantomData
        }
    }
}
 
impl<TIn: 'static + Send, Filter> PipeProcessor<TIn, TIn> for FilterPipeProcessor<TIn, Filter> 
    where   Filter: Fn(TIn) -> bool, TIn: Clone  {
 
    fn process_with_callback(&self, input: Option<TIn>, pipe: &Pipe<TIn,TIn>) -> () {
        
        let input_clone = input.clone();
        
        let pass_value = 
            match input {
                None => {
                    true
                },
                Some(inner_data) => {
                    // call filter function
                    (self.inner_filter)(inner_data) 
                },
            };
            
       if pass_value {
           pipe.process_out(input_clone);
       }
    }
}

pub struct ItemCache<TItem> where TItem: Clone {
    items: RwLock<Vec<Option<TItem>>>,
    is_empty: Mutex<bool>,
    empty_lock: Condvar
}

impl<TItem: 'static> ItemCache<TItem> where TItem: Clone {
    fn new() -> ItemCache<TItem> {
        ItemCache {
            items: RwLock::new(Vec::new()),
            is_empty: Mutex::new(true),
            empty_lock: Condvar::new()
        }
    }

    fn push(&self, item: Option<TItem>) {
        
        if item.is_none() {
            println!("pushing None");
        } else {
            println!("pushing Some");
        }
        
        let mut lock = self.items.write().unwrap();
        lock.push(item);
        
        if lock.len() == 1 {
           let mut is_empty = self.is_empty.lock().unwrap();
           *is_empty = false;
           
           self.empty_lock.notify_all(); 
        }
    }
    
    fn remove(&self) -> Option<TItem> {
        let mut is_empty = self.is_empty.lock().unwrap();
        let mut item : Option<TItem> = None;
        let mut got_item = false;
        
        while !got_item {
            if *is_empty {
                println!("is empty");
                is_empty = self.empty_lock.wait(is_empty).unwrap();
            } else {
                println!("getting item");
                let mut lock = self.items.write().unwrap();
                item = lock.remove(0);
        
                if lock.len() == 0 {
                    println!("removed last");
                    *is_empty = true;
                    self.empty_lock.notify_all(); 
                }
                got_item = true;
            }
        }

        item
    }    
}

pub struct ItemIterator<TItem> where TItem: Clone {
    item_cache: Arc<ItemCache<TItem>>
}

impl<TItem> ItemIterator<TItem> where TItem: Clone {
    pub fn new(item_cache: Arc<ItemCache<TItem>>) -> ItemIterator<TItem> {
        ItemIterator {
            item_cache: item_cache
        }
    }
}

impl<TItem: 'static> Iterator for ItemIterator<TItem> where TItem: Clone {
    type Item = TItem;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.item_cache.remove()
    }
}

pub struct GroupingPipeProcessor<TIn, TGroup>
    where TIn: Clone, TGroup: Clone {
    item_cache: Arc<ItemCache<TIn>>,
    _marker1: PhantomData<TIn>,
    _marker2: PhantomData<TGroup>
}

pub struct UngroupingPipeProcessor<TIn, TOut, TGroup>
    where TIn: Clone, TOut: Clone, TGroup: Clone  {
    _marker1: PhantomData<TIn>,
    _marker2: PhantomData<TOut>,
    _marker3: PhantomData<TGroup>
}

pub struct GroupReducerPipeProcessor<TInnerIn, TInnerOut, TGroup, ReducerFunction>
    where TInnerIn: Clone, TInnerOut: Clone, TGroup: Clone, 
    ReducerFunction: Fn(TGroup, Arc<ItemIterator<TInnerIn>>) -> TInnerOut, ReducerFunction: 'static + Send  {
    
    reducer_function: ReducerFunction,
    _marker1: PhantomData<TInnerIn>,
    _marker2: PhantomData<TInnerOut>,
    _marker3: PhantomData<TGroup>
}

pub struct GroupedMapPipeProcessor<TIn, TOut, TGroup>
    where TIn: Clone, TOut: Clone, TGroup: Clone  {
    _marker1: PhantomData<TIn>,
    _marker2: PhantomData<TOut>,
    _marker3: PhantomData<TGroup>
}

pub struct GroupIteratorPipeProcessor<TIn, TOut, TGroup>
    where TIn: Clone, TOut: Clone  {
    _marker1: PhantomData<TIn>,
    _marker2: PhantomData<TOut>,
    _marker3: PhantomData<TGroup>
}

pub struct ConsumerPipeProcessor<TIn, TOut>
    where TIn: Clone, TOut: Clone  {
    item_cache: Arc<ItemCache<TIn>>,
    _marker1: PhantomData<TIn>,
    _marker2: PhantomData<TOut>
}

impl<TIn: 'static + Send, TGroup: 'static> GroupingPipeProcessor<TIn, TGroup> 
    where TIn: Clone + Sync + Copy, TGroup: Clone + Hash + Eq + PartialEq  {
    
    pub fn new<GroupingFunction>(grouping_function: GroupingFunction, pipe: Arc<RwLock<Pipe<TIn,Arc<GroupedIterator<TIn, TGroup>>>>>) -> GroupingPipeProcessor<TIn, TGroup>
        where GroupingFunction: Fn(TIn) -> TGroup, GroupingFunction: 'static + Send {
        
        let item_cache:Arc<ItemCache<TIn>> = Arc::new(ItemCache::new());
        
        // spawn a thread to perform work in coordination with the main thread
        let item_cache_clone = item_cache.clone();
        
        let pipe_clone = pipe.clone();
        
        thread::spawn(move || {
            loop {
                // create a HashMap
                let mut group_map : HashMap<TGroup, Arc<GroupedIterator<TIn, TGroup>>> = HashMap::new();
                
                // create a new iterator
                let item_iterator = ItemIterator::new(item_cache_clone.clone());
                
                for item in item_iterator {
                    // determine the group
                    let group = grouping_function(item);
                    
                    if (group_map.contains_key(&group)) {
                        group_map.get(&group).unwrap().data.push(Some(item));
                    } else {
                        let grouped_iterator = Arc::new(GroupedIterator::new_with_first(group.clone(), item));
                            group_map.insert(group, grouped_iterator.clone());
                            
                            // and output it for processing
                            pipe_clone.read().unwrap().process_out(Some(grouped_iterator));
                    }
                }
                
                // signify end of batch
                for (group, group_iter) in &group_map {
                    // for each group
                    group_iter.data.push(None);
                }
                // then overall
                pipe_clone.read().unwrap().process_out(None);
            }
         });
        
        GroupingPipeProcessor {
            item_cache: item_cache,
            _marker1: PhantomData,
            _marker2: PhantomData
        }
    }
}

impl<TIn: 'static + Send, TGroup: 'static> PipeProcessor<TIn, Arc<GroupedIterator<TIn, TGroup>>> for GroupingPipeProcessor<TIn, TGroup> 
    where TIn: Clone, TGroup: Clone  {
 
    fn process_with_callback(&self, input: Option<TIn>, pipe: &Pipe<TIn,Arc<GroupedIterator<TIn, TGroup>>>) -> () {
        // push the input into the ItemCache where it will be consumed through the iterator
        self.item_cache.push(input);
    }
}

impl<TInnerIn: 'static + Send, TInnerOut: 'static + Send, TGroup: 'static, ReducerFunction> GroupReducerPipeProcessor<TInnerIn, TInnerOut, TGroup, ReducerFunction> 
    where   TInnerIn: Clone + Sync + Copy, TInnerOut: 'static + Send + Clone, TGroup: Clone + Hash + Eq + PartialEq,  
            ReducerFunction: Fn(TGroup, Arc<ItemIterator<TInnerIn>>) -> TInnerOut, ReducerFunction: 'static + Send {
    pub fn new(reducer_function: ReducerFunction, pipe: Arc<RwLock<Pipe<GroupedIterator<TInnerIn, TGroup>, GroupedData<TInnerOut, TGroup>>>>) -> GroupReducerPipeProcessor<TInnerIn, TInnerOut, TGroup,ReducerFunction>
         {
        GroupReducerPipeProcessor {
            reducer_function: reducer_function,
            _marker1: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData
        }
    }
}

impl<TInnerIn: 'static + Send, TInnerOut: 'static + Send, TGroup: 'static, ReducerFunction> PipeProcessor<GroupedIterator<TInnerIn,TGroup>,GroupedData<TInnerOut,TGroup>> for GroupReducerPipeProcessor<TInnerIn, TInnerOut, TGroup, ReducerFunction>
    where   TInnerIn: Clone + Sync + Copy, TInnerOut: 'static + Send + Clone, TGroup: Clone + Hash + Eq + PartialEq,  
            ReducerFunction: Fn(TGroup, Arc<ItemIterator<TInnerIn>>) -> TInnerOut, ReducerFunction: 'static + Send {
    
    fn process_with_callback(&self, input: Option<GroupedIterator<TInnerIn,TGroup>>, pipe: &Pipe<GroupedIterator<TInnerIn,TGroup>,GroupedData<TInnerOut,TGroup>>) -> () {
        if input.is_some() {
            
            let unwrapped = input.unwrap();
            
            let grouping_key = unwrapped.clone().grouping_key;
            let iterator = unwrapped.clone().iterator;
            
            let reduced_group = GroupedData::new(grouping_key.clone(), (self.reducer_function)(grouping_key.clone(),iterator));
            pipe.process_out(Some(reduced_group));
        } else {
            pipe.process_out(None);    
        }
    }
}

impl<TIn: 'static, TOut: 'static> ConsumerPipeProcessor<TIn, TOut>
    where TIn: Clone + Sync + Send, TOut: Clone  {
    
    pub fn new<ConsumerFunction>(consumer_function: ConsumerFunction, pipe: Arc<RwLock<Pipe<TIn,TOut>>>) -> ConsumerPipeProcessor<TIn, TOut>
        where
            ConsumerFunction: Fn(ItemIterator<TIn>) -> TOut, ConsumerFunction: 'static + Send {
        
        let item_cache:Arc<ItemCache<TIn>> = Arc::new(ItemCache::new());
        
        // spawn a thread to perform work in coordination with the main thread
        let item_cache_clone = item_cache.clone();
        
        let pipe_clone = pipe.clone();
        
        thread::spawn(move || {
            loop {
                // create a new iterator
                let item_iterator = ItemIterator::new(item_cache_clone.clone());
                
                let value = consumer_function(item_iterator);
                
                pipe_clone.read().unwrap().process_out(Some(value));
                // signify end of batch
                pipe_clone.read().unwrap().process_out(None);
            }
         });
        
        ConsumerPipeProcessor {
            item_cache: item_cache,
            _marker1: PhantomData,
            _marker2: PhantomData
        }
    }
}

impl<TIn: 'static + Send, TOut: 'static> PipeProcessor<TIn, TOut> for ConsumerPipeProcessor<TIn, TOut> 
    where TIn: Clone, TOut: Clone  {
 
    fn process_with_callback(&self, input: Option<TIn>, pipe: &Pipe<TIn,TOut>) -> () {
        // push the input into the ItemCache where it will be consumed through the iterator
        self.item_cache.push(input);
    }
}

pub struct IteratorPipeProcessor<TIn, TOut>
    where TIn: Clone, TOut: Clone  {
    item_cache: Arc<ItemCache<TIn>>,
    _marker1: PhantomData<TIn>,
    _marker2: PhantomData<TOut>
}

impl<TIn: 'static, TOut: 'static> IteratorPipeProcessor<TIn, TOut>
    where TIn: Clone + Sync + Send, TOut: Clone  {
    
    pub fn new<IterFunction, TIterator>(iter_function: IterFunction, pipe: Arc<RwLock<Pipe<TIn,TOut>>> ) -> IteratorPipeProcessor<TIn, TOut>
        where
            TIterator: Iterator<Item=TOut>,
            IterFunction: Fn(ItemIterator<TIn>) -> TIterator, IterFunction: 'static + Send {
        
        // need work here
        let item_cache:Arc<ItemCache<TIn>> = Arc::new(ItemCache::new());
        
        // spawn a thread to perform work in coordination with the main thread
        let item_cache_clone = item_cache.clone();
        
        let pipe_clone = pipe.clone();
        
        thread::spawn(move || {
            loop {
                // create a new iterator
                let item_iterator = ItemIterator::new(item_cache_clone.clone());
                
                //let gen_iter = Arc::try_unwrap(iter_function(item_iterator)).ok().unwrap();
                let mut gen_iter = iter_function(item_iterator);
            
                // iterate until end of batch
                for item in gen_iter {
                    pipe.read().unwrap().process_out(Some(item));
                }
                
                // signify end of batch
                pipe_clone.read().unwrap().process_out(None);
            }
         });
        
        IteratorPipeProcessor {
            item_cache: item_cache,
            _marker1: PhantomData,
            _marker2: PhantomData
        }
    }
}

impl<TIn: 'static + Send, TOut: 'static> PipeProcessor<TIn, TOut> for IteratorPipeProcessor<TIn, TOut> 
    where TIn: Clone, TOut: Clone  {
 
    fn process_with_callback(&self, input: Option<TIn>, pipe: &Pipe<TIn,TOut>) -> () {
        // push the input into the ItemCache where it will be consumed through the iterator
        self.item_cache.push(input);
    }
}

pub struct FlatMapPipeProcessor<TIn, TOut, FlatMapFunction> 
    where   FlatMapFunction: Fn(TIn) -> Box<Iterator<Item=TOut>>, TIn: Clone, TOut: Clone  {
    flatmap_function: FlatMapFunction,
    _marker1: PhantomData<TIn>,
    _marker2: PhantomData<TOut>
}

impl<TIn: 'static + Send, TOut: 'static, FlatMapFunction: 'static> FlatMapPipeProcessor<TIn, TOut, FlatMapFunction>
    where   FlatMapFunction: Fn(TIn) -> Box<Iterator<Item=TOut>>, TIn: Clone, TOut: Clone {

    pub fn new(flatmap_function: FlatMapFunction) -> FlatMapPipeProcessor<TIn, TOut, FlatMapFunction> {
        FlatMapPipeProcessor {
            flatmap_function: flatmap_function,
            _marker1: PhantomData,
            _marker2: PhantomData
        }
    }
}
 
impl<TIn: 'static + Send, TOut: 'static, FlatMapFunction> PipeProcessor<TIn, TOut> for FlatMapPipeProcessor<TIn, TOut, FlatMapFunction> 
    where   FlatMapFunction: Fn(TIn) -> Box<Iterator<Item=TOut>>, TIn: Clone, TOut: Clone  {
 
    fn process_with_callback(&self, input: Option<TIn>, pipe: &Pipe<TIn,TOut>) -> () {
        
        let input_clone = input.clone();
        
        match input {
            None => {
                pipe.process_out(None);
            },
            Some(inner_data) => {
                // call flatmap function
                let iterator = (self.flatmap_function)(inner_data);
                
                for data in iterator {
                    pipe.process_out(Some(data));
                }
            },
        };
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

impl<I: 'static + Send, O: 'static> Processor<I> for Pipe<I, O> where I: Clone, O: Clone {
    
    fn process(&self, data: Option<I>) -> () {
        let data = self.process_in(data);
        
        if data.is_some() {
            // count normal inputs
            let mut in_count = self.processing_state.in_count.write().unwrap();
            *in_count += 1;
        }
        
        self.pipe_processor.process_with_callback(data, &self);
    }
    
    fn has_work_remaining(&self) -> bool {
        let mut has_work = false;
        
        let in_count = *(self.processing_state.in_count.read().unwrap().deref());
        let out_count = *(self.processing_state.out_count.read().unwrap().deref());
        
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
        *(self.processing_state.out_count.read().unwrap().deref())
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
    
    ( $threads:expr, iter $nexty:ty => $next:ident ) => {
        {
            Connector::<$nexty>::sync_connector($next.clone())
        }
    };
    
    ( $threads:expr, consumer $nexty:ty => $next:ident ) => {
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
    ( pipe => sync $next:expr ) => {
        {
            {
                Arc::new(RwLock::new(Pipe::new($next)))
            }
        }
    };
    
    ( pipe => async $next:expr ) => {
        {
            {
                Arc::new(RwLock::new(Pipe::new($next)))
            }
        }
    };
    
    ( pipe => iter $next:expr ) => {
        {
            {
                Pipe::iter($next)
            }
        }
    };
    
    ( pipe => consumer $next:expr ) => {
        {
            {
                Pipe::consumer($next)
            }
        }
    };
    
    ( single => $threads:expr, $last:ident, $mode:tt $nexty:ty => $next:expr ) => {
        {
            let nextpipe = connect!(pipe => $mode $next);
            let connector = connector!($threads, $mode $nexty => nextpipe);
            
            $last.write().unwrap().connect(connector);
            nextpipe.clone()
        }
    };
    
    ( $threads:expr, $last:ident, $($mode:tt $nexty:ty => $next:expr ),* ) => {
        {
            let last_pipe = $last.clone();
                    
            $(
                let last_pipe = connect!(single => $threads, last_pipe, $mode $nexty => $next);
            )*
            
            last_pipe
        }
    };
}

macro_rules! pipeline {
    ( async_connector_threads: $threads:expr, $headty:ty => $head:expr, $($mode:tt $nexty:ty => $next:expr),* => $outty:ty) => {
        {
            let pipe = Arc::new(RwLock::new(Pipe::new($head)));
            let last = pipe.clone();
            
            let last = connect!($threads, last, $($mode $nexty => $next),*);
            
            Pipeline::new(pipe, last.clone())
        }
    };
        
    ( $headty:ty => $head:expr, $($mode:tt $nexty:ty => $next:expr),* => $outty:ty) => {
        {
            pipeline!(async_connector_threads: 4, $headty => $head, $($mode $nexty => $next),* => $outty:ty)
        }
    };
}

/*
fn pipe_macro_expand() {
        let pipeline = pipeline!(async_connector_threads: 8,
                            i64 => |x:i64|{x*2},
                            sync i64 => |x:i64|{x*10},
                            //iter i64 => |iter| { iter.filter(|n| {*n>=40}) },
                            sync i64 => |x:i64|{x+2},
                            async i64 => |x:i64| { 
                                let result = x*9;
                                println!("pipe_macro Result is {}", result);
                                TestResult {
                                    depth: 0,
                                    result: result
                                }
                            }
                            => TestResult
                            );
}
*/
  
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, RwLock};
    use std::thread;
    use std::thread::{sleep};
    use time::{Duration};
    use core::ops::Deref;
    use std::time::Duration as StdDuration;
    
    
    fn double(i:i64) -> i64 {
        //println!("double");
        //format!("Value before double is: {}",i);
        i*2
    }
    fn triple(i:i64) -> i64 {
        //println!("triple");
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
                            i64 => |x:i64| {x*2},
                            sync i64 => |x:i64| {x*10},
                            iter i64 => |iter| { 
                                println!("creating iterator.");
                                iter.filter(|n| {*n>40} ) 
                            },
                            consumer i64 => |iter| { iter.max().unwrap() },
                            async i64 => |x:i64| { 
                                let result = x*2;
                                println!("pipe_macro Result is {}", result);
                                TestResult {
                                    depth: 0,
                                    result: result
                                }
                            }
                            => TestResult
                            );
                          
       pipeline.process(2);
       pipeline.process(2);
       pipeline.process(2);
       pipeline.process(2);
       pipeline.process(8);
       pipeline.process(5);
       pipeline.process(6);
       pipeline.process(1);
       
       println!("Flush and wait....");
                                
       pipeline.flush_and_wait(StdDuration::from_millis(10000));
       println!("Done....");
       let last_pipe = pipeline.pipe_out.read().unwrap();
       
       let complete_batch_count = *(last_pipe.processing_state.complete_batch_count.lock().unwrap().deref());
       assert_eq!(1, complete_batch_count);
    }
    
    #[test]
    fn pipe() {
        println!("starting");
        // create a Pipe
        let double_pipe = Arc::new(RwLock::new(Pipe::new(double)));
        let filter_pipe = Arc::new(RwLock::new(Pipe::<i64,i64>::filter(|x:i64| { x >= 40 })));
        let triple_pipe = Arc::new(RwLock::new(Pipe::new(triple)));
        let second_double_pipe = Arc::new(RwLock::new(Pipe::new(double)));
        
        let other_pipe = Arc::new(RwLock::new(Pipe::new(|x:i64| { 
                let result = x*10;
                //println!("Result is {}", result);
                sleep(StdDuration::from_millis(2000));
                TestResult {
                    depth: 0,
                    result: result
                }
               })));
               
        let branch_pipe = Arc::new(RwLock::new(Pipe::new(|x:i64| { 
                let result = x*5;
                //println!("Result  on branch is {}", result);
                result
               })));
        
        let val:i64 = 55;
        let connector0 = Connector::<i64>::sync_connector(filter_pipe.clone());
        let connector1 = Connector::<i64>::sync_connector(triple_pipe.clone());
        let connector2 = Connector::<i64>::async_connector(second_double_pipe.clone(), 4);
        let connector3 = Connector::<i64>::sync_connector(other_pipe.clone());
        let branch_connector = Connector::<i64>::async_connector(branch_pipe.clone(), 2);
        let queue_connector = Connector::<TestResult>::queue_connector();
        
        // connect the pipes
        double_pipe.write().unwrap().connect(connector0);
        filter_pipe.write().unwrap().connect(connector1);
        triple_pipe.write().unwrap().connect(connector2);
        second_double_pipe.write().unwrap().connect(connector3);
        //triple_pipe.write().unwrap().connect(branch_connector);
        other_pipe.write().unwrap().connect(queue_connector);
        
        double_pipe.write().unwrap().process(Some(val));
        double_pipe.write().unwrap().process(Some(50));
        double_pipe.write().unwrap().process(Some(36));
        
        let pipeline = Pipeline::new(double_pipe, other_pipe);
        
        pipeline.process(val);
        pipeline.process(50);
        pipeline.process(36);
        
        pipeline.flush_and_wait(StdDuration::from_millis(10000));
                
        for i in 0..1 {
            let last_pipe = pipeline.pipe_out.read().unwrap();
            let last_connectors = last_pipe.out_connectors.read().unwrap();
            let last_connector = last_connectors.first().unwrap();
            
            let result = last_connector.receive();
            if result.is_ok() {
             println!("In final loop {}.. result is {}", i, result.ok().unwrap().result);
            }
        }
        
        //let completed = double_pipe.write().unwrap().wait_work_complete(Duration::milliseconds(10000));
        
        //println!("Work completed {}", completed);
    }
    
    #[test]
    fn pipe_with_iterators() {
        println!("starting");
        // create a Pipe
        let double_pipe = Arc::new(RwLock::new(Pipe::new(double)));
        let filter_pipe = Arc::new(RwLock::new(Pipe::<i64,i64>::stub()));
        
        let iter_processor = Arc::new(IteratorPipeProcessor::new(|iter| { iter.filter(|n:&i64| {*n>=40}) } ,filter_pipe.clone()));
          
        filter_pipe.write().unwrap().set_pipe_processor(iter_processor);
        
        let triple_pipe = Arc::new(RwLock::new(Pipe::new(triple)));
        let second_double_pipe = Arc::new(RwLock::new(Pipe::new(double)));
        
        let other_pipe = Arc::new(RwLock::new(Pipe::new(|x:i64| { 
                let result = x*10;
                //println!("Result is {}", result);
                sleep(StdDuration::from_millis(2000));
                TestResult {
                    depth: 0,
                    result: result
                }
               })));
               
        let branch_pipe = Arc::new(RwLock::new(Pipe::new(|x:i64| { 
                let result = x*5;
                //println!("Result  on branch is {}", result);
                result
               })));
        
        let val:i64 = 55;
        let connector0 = Connector::<i64>::sync_connector(filter_pipe.clone());
        let connector1 = Connector::<i64>::sync_connector(triple_pipe.clone());
        let connector2 = Connector::<i64>::async_connector(second_double_pipe.clone(), 4);
        let connector3 = Connector::<i64>::sync_connector(other_pipe.clone());
        let branch_connector = Connector::<i64>::async_connector(branch_pipe.clone(), 2);
        let queue_connector = Connector::<TestResult>::queue_connector();
        
        // connect the pipes
        double_pipe.write().unwrap().connect(connector0);
        filter_pipe.write().unwrap().connect(connector1);
        triple_pipe.write().unwrap().connect(connector2);
        second_double_pipe.write().unwrap().connect(connector3);
        //triple_pipe.write().unwrap().connect(branch_connector);
        other_pipe.write().unwrap().connect(queue_connector);
                
        let pipeline = Pipeline::new(double_pipe, other_pipe);
        pipeline.process(val);
        pipeline.process(50);
        pipeline.process(36);
        
        pipeline.flush_and_wait(StdDuration::from_millis(10000));
               
        for i in 0..1 {
            let last_pipe = pipeline.pipe_out.read().unwrap();
            let last_connectors = last_pipe.out_connectors.read().unwrap();
            let last_connector = last_connectors.first().unwrap();
            
            let result = last_connector.receive();
            
            if result.is_ok() {
             println!("In final loop {}.. result is {}", i, result.ok().unwrap().result);
            }
        }
        
        //let completed = double_pipe.write().unwrap().wait_work_complete(Duration::milliseconds(10000));
        
        //println!("Work completed {}", completed);
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