use std::cmp::Ordering;

/// Holds a list of disjunctive [start, end) segments covering the received file
pub struct Segments(Vec<(u64, u64)>);
impl Segments {
    pub fn new() -> Self {
        Segments(Vec::new())
    }

    /// return the number of disjunctive segments
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// return the end of the last segment or None if there is no segment
    pub fn end(&self) -> Option<u64> {
        self.0.last().map(|x| x.1)
    }

    /// return the end of the last segment or 0 if there is no segment
    pub fn end_or_0(&self) -> u64 {
        self.0.last().map(|x| x.1).unwrap_or(0)
    }

    /// return true if there is only one segment whose end is the given size
    pub fn is_complete(&self, size: u64) -> bool {
        self.0.len() == 1 && self.0[0].1 == size
    }

    /// update the contiguous segment list with the new segment, filling any gap (i.e. unifying segments) if required
    /// return the number of new bytes added (e.g. if the new segment overlaps completely existing segments, return 0)
    pub fn merge(&mut self, seg: (u64, u64)) -> u64 {
        assert!(seg.0 < seg.1, "invalid segment");
        let v = &mut self.0;

        let len = v.len();
        if len == 0 {
            v.push(seg);
            return seg.1 - seg.0;
        }
        let mut newly_received = 0;

        let last = &mut v[len - 1];
        match last.1.cmp(&seg.0) {
            Ordering::Equal => {
                //most probable case - the data fits nicely at the end
                last.1 = seg.1;
                newly_received = seg.1 - seg.0;
            }
            Ordering::Less => {
                //the new segment comes at the end of the list after a gap
                v.push(seg);
                newly_received = seg.1 - seg.0
            }
            Ordering::Greater => {
                // data fills some gap in the middle, use binary search to find where it should go
                match v.binary_search_by(|x| x.0.cmp(&seg.0)) {
                    Ok(k) => {
                        // the segment starts at the same offset with the segment v[k]
                        if v[k].1 < seg.1 {
                            newly_received = seg.1 - v[k].1;
                            v[k].1 = seg.1;
                            newly_received -= merge(v, k);
                        }
                    }
                    Err(k) => {
                        if k == 0 {
                            //new segment comes right at the beginning of the list
                            if seg.1 < v[0].0 {
                                v.insert(0, seg);
                                newly_received = seg.1 - seg.0;
                            } else {
                                //overlapping with the first segment
                                newly_received = v[0].0 - seg.0;
                                v[0].0 = seg.0;
                                if seg.1 > v[0].1 {
                                    newly_received += seg.1 - v[0].1;
                                    v[0].1 = seg.1;
                                    merge(v, 0);
                                }
                            }
                        } else {
                            //here we know there is an element to the left
                            if v[k - 1].1 >= seg.0 {
                                //overlaps with the left
                                if v[k - 1].1 < seg.1 {
                                    newly_received = seg.1 - v[k - 1].1;
                                    v[k - 1].1 = seg.1;
                                    newly_received -= merge(v, k - 1);
                                } //else seg is completely embedded in v[k-1]
                            } else {
                                //does not overlap with left
                                //we know there is an element to the right (i.e. k < len),
                                // otherwise this would be the last in the list and we have tested for that
                                // in the match Ordering::Less above
                                if seg.1 < v[k].0 {
                                    //does not overlap with right either
                                    v.insert(k, seg);
                                    newly_received = seg.1 - seg.0;
                                } else {
                                    //overlaps with right, we have to extend the right to the left
                                    newly_received = v[k].0 - seg.0;
                                    v[k].0 = seg.0;
                                    if v[k].1 < seg.1 {
                                        newly_received += seg.1 - v[k].1;
                                        v[k].1 = seg.1;
                                        newly_received -= merge(v, k);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        newly_received
    }

    /// return a list of gaps covering the [start, end) interval
    pub fn gaps(&self, start: u64, end: u64) -> Vec<(u64, u64)> {
        let v = &self.0;

        let mut gaps = Vec::new();

        let (idx, mut pointer) = match v.binary_search_by(|x| x.0.cmp(&start)) {
            Ok(k) => (k + 1, v[k].1),
            Err(k) => (
                k,
                if k == 0 {
                    start
                } else {
                    std::cmp::max(v[k - 1].1, start)
                },
            ),
        };

        for (s, e) in &v[idx..] {
            if *s >= end {
                gaps.push((pointer, end));
                pointer = end;
                break;
            }

            gaps.push((pointer, *s));
            pointer = *e;

            if pointer > end {
                break;
            }
        }

        if pointer < end {
            gaps.push((pointer, end));
        }
        gaps
    }
}

/// v[k] has been enlarged to the right possibly overlapping with its right segments
/// this function merges all the overlapping segments reducing the size of the list
///
/// return the size of overlapping data
fn merge(v: &mut Vec<(u64, u64)>, k: usize) -> u64 {
    // it is very unlikely this will loop more than once so no need to bother removing in bulk
    let mut overlapping = 0;
    while k + 1 < v.len() && v[k + 1].0 <= v[k].1 {
        if v[k + 1].1 > v[k].1 {
            overlapping += v[k].1 - v[k + 1].0;
            v[k].1 = v[k + 1].1
        } else {
            overlapping += v[k + 1].1 - v[k + 1].0;
        }

        v.remove(k + 1);
    }

    overlapping
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test1() {
        let mut s = Segments::new();
        assert_eq!(s.gaps(0, 30), vec![(0, 30)]);

        assert_eq!(10, s.merge((0, 10)));
        assert_eq!(10, s.merge((10, 20)));

        assert_eq!(s.0, vec![(0, 20)]);

        assert_eq!(s.gaps(0, 20), vec![]);
        assert_eq!(s.gaps(0, 30), vec![(20, 30)]);
    }

    #[test]
    fn test2() {
        let mut s = Segments::new();
        assert_eq!(10, s.merge((10, 20)));

        assert_eq!(s.gaps(0, 10), vec![(0, 10)]);
        assert_eq!(s.gaps(0, 15), vec![(0, 10)]);
        assert_eq!(s.gaps(0, 25), vec![(0, 10), (20, 25)]);

        assert_eq!(10, s.merge((0, 10)));

        assert_eq!(s.0, vec![(0, 20)]);
    }

    #[test]
    fn test3() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((0, 10)));
        assert_eq!(10, s.merge((20, 30)));
        assert_eq!(2, s.len());

        assert_eq!(s.gaps(10, 20), vec![(10, 20)]);
        assert_eq!(s.gaps(5, 30), vec![(10, 20)]);
        assert_eq!(s.gaps(5, 35), vec![(10, 20), (30, 35)]);

        assert_eq!(10, s.merge((10, 20)));

        assert_eq!(s.0, vec![(0, 30)]);
    }

    #[test]
    fn test4() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((0, 10)));
        assert_eq!(10, s.merge((20, 30)));
        assert_eq!(2, s.len());

        assert_eq!(3, s.merge((12, 15)));

        assert_eq!(s.0, vec![(0, 10), (12, 15), (20, 30)]);
    }

    #[test]
    fn test5() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((0, 10)));
        assert_eq!(10, s.merge((20, 30)));
        assert_eq!(2, s.len());

        assert_eq!(8, s.merge((12, 20)));

        assert_eq!(s.0, vec![(0, 10), (12, 30)]);
    }

    #[test]
    fn test6() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((0, 10)));
        assert_eq!(10, s.merge((0, 20)));

        assert_eq!(s.0, vec![(0, 20)]);
    }

    #[test]
    fn test7() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((10, 20)));
        assert_eq!(5, s.merge((0, 5)));

        assert_eq!(s.0, vec![(0, 5), (10, 20)]);
    }

    #[test]
    fn test8() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((0, 10)));
        assert_eq!(10, s.merge((20, 30)));
        assert_eq!(5, s.merge((10, 15)));

        assert_eq!(s.0, vec![(0, 15), (20, 30)]);
    }

    #[test]
    fn test9() {
        let mut s = Segments::new();

        assert_eq!(30, s.merge((0, 30)));
        assert_eq!(0, s.merge((10, 20)));

        assert_eq!(s.0, vec![(0, 30)]);
    }

    //these are the weird cases where a segment larger than the previous ones comes in

    #[test]
    fn test10() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((0, 10)));
        assert_eq!(10, s.merge((0, 20)));
        assert_eq!(s.0, vec![(0, 20)]);

        assert_eq!(0, s.merge((10, 15)));

        assert_eq!(s.0, vec![(0, 20)]);
    }

    #[test]
    fn test11() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((0, 10)));
        assert_eq!(10, s.merge((20, 30)));
        assert_eq!(10, s.merge((40, 50)));

        assert_eq!(s.0, vec![(0, 10), (20, 30), (40, 50)]);

        assert_eq!(20, s.merge((5, 45)));

        assert_eq!(s.0, vec![(0, 50)]);
    }

    #[test]
    fn test12() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((0, 10)));
        assert_eq!(10, s.merge((20, 30)));

        assert_eq!(s.0, vec![(0, 10), (20, 30)]);

        assert_eq!(25, s.merge((5, 45)));

        assert_eq!(s.0, vec![(0, 45)]);
    }

    #[test]
    fn test13() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((10, 20)));
        assert_eq!(15, s.merge((5, 30)));

        assert_eq!(s.0, vec![(5, 30)]);
    }

    #[test]
    fn test14() {
        let mut s = Segments::new();

        assert_eq!(10, s.merge((0, 10)));
        assert_eq!(10, s.merge((20, 30)));
        assert_eq!(10, s.merge((15, 35)));

        assert_eq!(s.0, vec![(0, 10), (15, 35)]);
    }
    #[test]
    #[should_panic]
    fn test_invalid_segment() {
        let mut v = Segments::new();

        v.merge((10, 10));
    }
}
