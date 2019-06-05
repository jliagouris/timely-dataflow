extern crate timely;

use std::collections::HashMap;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Operator, Inspect, Probe};
use timely::dataflow::channels::pact::Exchange;
use timely::state::backends::{FASTERBackend,InMemoryBackend};
use faster_rs::FasterKv;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // define a distribution function for strings.
        let exchange = Exchange::new(|x: &(String, i64)| (x.0).len() as u64);

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize,_,_,FASTERBackend>(|scope| {
            input.to_stream(scope)
                 .flat_map(|(text, diff): (String, i64)|
                    text.split_whitespace()
                        .map(move |word| (word.to_owned(), diff))
                        .collect::<Vec<_>>()
                 )
                 .unary_frontier(exchange, "WordCount", |_capability, _info, state_handle| {
                    let mut queues = HashMap::new();
                     let inner_faster = FasterKv::new(1<<15, 1024*1024*1024, "storage".to_string()).unwrap();
                     let mut sequence_num = 0;

                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            queues.entry(time.retain())
                                  .or_insert(Vec::new())
                                  .push(data.replace(Vec::new()));
                        }

                        for (key, val) in queues.iter_mut() {
                            if !input.frontier().less_equal(key.time()) {
                                let mut session = output.session(key);
                                for mut batch in val.drain(..) {
                                    for (word, diff) in batch.drain(..) {
                                        let mut count = state_handle.get_managed_count(&word.clone());
                                        count.increase(diff);
                                        session.give((word.clone(), count.get()));
                                        /*
                                        inner_faster.rmw(&word, &count.get(), sequence_num);
                                        sequence_num += 1;
                                        if sequence_num % (1 << 14) == 0 {
                                            let check = inner_faster.checkpoint().unwrap();
                                            println!("Calling checkpoint with token {}", check.token);
                                        }
                                        else if sequence_num % (1 << 8) == 0 {
                                            inner_faster.complete_pending(false);
                                        } else if sequence_num % (1 << 5) == 0 {
                                            inner_faster.refresh();
                                        }
                                        */
                                    }
                                }
                            }
                        }

                        queues.retain(|_key, val| !val.is_empty());
                    }})
                 //.inspect(|x| println!("seen: {:?}", x))
                 .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..(1<<25) {
            input.send(("round".to_owned(), 1));
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
