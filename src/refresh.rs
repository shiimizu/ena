/// Helper method to create an iterator that mimics the thread refresh system  
///
/// It repeats indefintely so `take` is required to limit how many `step_by`.  
/// If the iterator has reached or passed its last value, it will keep repeating that last value.  
///
/// # Arguments
///
/// * `initial` - Initial value
/// * `step_by` - Add this much every time `next()` is called
/// * `take`    - Limit this to how many additions to make from `step_by`
///
/// # Example
///
/// ```
/// let mut rate = refresh_rate(30000, 5 * 1000, 10);
/// assert_eq!(Some(30000), rate.next());
/// assert_eq!(Some(35000), rate.next());
/// assert_eq!(Some(40000), rate.next());
/// assert_eq!(Some(45000), rate.next());
/// assert_eq!(Some(50000), rate.next());
/// assert_eq!(Some(55000), rate.next());
/// assert_eq!(Some(60000), rate.next());
/// assert_eq!(Some(65000), rate.next());
/// assert_eq!(Some(70000), rate.next());
/// assert_eq!(Some(75000), rate.next()); // hit the upper limit. repeat.
///
/// assert_eq!(Some(75000), rate.next());
/// assert_eq!(Some(75000), rate.next());
/// rate.reset();
/// assert_eq!(Some(30000), rate.next());
/// assert_eq!(Some(35000), rate.next());
/// ```
pub fn refresh_rate(
    initial: u64,
    step_by: usize,
    take: usize,
) -> Reset<impl Clone + Iterator<Item = u64>> {
    let mut base = (initial..).step_by(step_by).take(take);
    let repeat = std::iter::repeat(base.clone().last().unwrap());
    base.chain(repeat).resettable()
}

/// An iterator with a `reset()` that sets the position of the iterator back to the start.
///
/// This `struct` is created by the [`resettable`] method on [`Iterator`]. See its
/// documentation for more.
///
/// [`resettable`]: ResetExt::resettable
#[derive(Copy, Clone, Debug)]
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct Reset<I> {
    orig: I,
    iter: I,
}
impl<I: Clone> Reset<I> {
    pub(super) fn new(iter: I) -> Reset<I> {
        Reset {
            orig: iter.clone(),
            iter,
        }
    }
}

impl<I: Clone + Iterator> Reset<I> {
    /// Sets the position of the iterator back to the start
    ///
    /// ```
    /// let mut iter = (0..10).resettable();
    /// assert_eq!(Some(0), iter.next());
    /// assert_eq!(Some(1), iter.next());
    /// assert_eq!(Some(2), iter.next());
    /// iter.reset();
    /// assert_eq!(Some(0), iter.next());
    /// ```
    #[inline]
    pub fn reset(&mut self) {
        self.iter = self.orig.clone();
    }
}

/// Applies the [`Reset`] adapter to [`Iterator`] types.
pub trait ResetExt {
    /// Creates an iterator which can use `reset` to set the position of
    /// the iterator back to the start.
    ///
    /// Adds a [`reset`] method to an iterator. See its documentation for
    /// more information.
    ///
    /// [`reset`]: Reset::reset
    /// [`next`]: Iterator::next
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// let mut iter = (0..10).resettable();
    /// assert_eq!(Some(0), iter.next());
    /// assert_eq!(Some(1), iter.next());
    /// assert_eq!(Some(2), iter.next());
    /// iter.reset();
    /// assert_eq!(Some(0), iter.next());
    /// ```
    fn resettable(self) -> Reset<Self>
    where
        Self: Sized + Clone,
    {
        Reset::new(self)
    }
}

impl<T: Clone + Iterator> ResetExt for T {}

impl<I> Iterator for Reset<I>
where
    I: Clone + Iterator,
{
    type Item = <I as Iterator>::Item;

    #[inline]
    fn next(&mut self) -> Option<<I as Iterator>::Item> {
        self.iter.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reset() {
        let mut iter = (0..10).resettable();
        assert_eq!(Some(0), iter.next());
        assert_eq!(Some(1), iter.next());
        assert_eq!(Some(2), iter.next());
        iter.reset();
        assert_eq!(Some(0), iter.next());
    }

    #[test]
    fn refresh_rate_reset() {
        let mut rate = refresh_rate(30000, 5 * 1000, 10);
        assert_eq!(Some(30000), rate.next());
        assert_eq!(Some(35000), rate.next());
        rate.reset();
        assert_eq!(Some(30000), rate.next());
        assert_eq!(Some(35000), rate.next());
    }
    #[test]
    fn refresh_rate_poll_next() {
        let take = 10;
        let mut rate = refresh_rate(30000, 5 * 1000, take);
        assert_eq!(Some(30000), rate.next());
        assert_eq!(Some(35000), rate.next());
        assert_eq!(Some(75000), rate.nth(take - 1));
    }
}
