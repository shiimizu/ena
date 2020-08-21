use std::iter::{Chain, Repeat, StepBy, Take};

/// Create an iterator that mimics the thread refresh system  
///
/// It repeats indefintely so `take` is required to limit how many `step_by`.  
/// If the stream has reached or passed its last value, it will keep repeating that last value.  
/// If an initial `refreshDelay` was set to `20`, `5` is added to each and subsequent requests
/// that return `NOT_MODIFIED`. If the next request is `OK`, the stream can be reset back to it's
/// initial value by calling `clone()` on it.
///
/// # Arguments
///
/// * `initial` - Initial value in seconds
/// * `step_by` - Add this much every time `next()` is called
/// * `take`    - Limit this to how many additions to make from `step_by`
///
/// # Example
///
/// ```
/// use ena::config;
/// let orig = config::refresh_rate(20, 5, 10);
/// let mut rate = orig.clone();
/// rate.next(); // 20
/// rate.next(); // 25
/// rate.next(); // 30
///
/// /* continued calls to rate.next(); */
/// rate.next(); // 75
/// rate.next(); // 75 .. repeating
///
/// rate = orig.clone();
/// rate.next(); // 20
/// ```
pub fn refresh_rate(initial: u64, step_by: usize, take: usize) -> Chain<Take<StepBy<std::ops::RangeFrom<u64>>>, Repeat<u64>> {
    let base = (initial..).step_by(step_by).take(take);
    let repeat = std::iter::repeat(base.clone().last().unwrap());
    let ratelimit = base.chain(repeat);
    ratelimit
}
