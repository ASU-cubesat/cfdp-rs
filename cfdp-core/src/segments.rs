use std::cmp::Ordering;

/// update the contiguous segment list with the new segment, filling any gap (i.e. unifying segments) if required
/// return the number of new bytes received (e.g. if the segment overlaps completely existing segmens, return 0)
pub fn update_segments(v: &mut Vec<(u64, u64)>, seg: (u64, u64)) -> u64 {
    assert!(seg.0 < seg.1, "invalid segment");

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
            //there was just a data loss and now a new segment comes at the end of the list
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
        let mut v = vec![];
        assert_eq!(10, update_segments(&mut v, (0, 10)));
        assert_eq!(10, update_segments(&mut v, (10, 20)));

        assert_eq!(v, vec![(0, 20)]);
    }

    #[test]
    fn test2() {
        let mut v = vec![];
        assert_eq!(10, update_segments(&mut v, (10, 20)));
        assert_eq!(10, update_segments(&mut v, (0, 10)));

        assert_eq!(v, vec![(0, 20)]);
    }

    #[test]
    fn test3() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (0, 10)));
        assert_eq!(10, update_segments(&mut v, (20, 30)));
        assert_eq!(2, v.len());

        assert_eq!(10, update_segments(&mut v, (10, 20)));

        assert_eq!(v, vec![(0, 30)]);
    }

    #[test]
    fn test4() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (0, 10)));
        assert_eq!(10, update_segments(&mut v, (20, 30)));
        assert_eq!(2, v.len());

        assert_eq!(3, update_segments(&mut v, (12, 15)));

        assert_eq!(v, vec![(0, 10), (12, 15), (20, 30)]);
    }

    #[test]
    fn test5() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (0, 10)));
        assert_eq!(10, update_segments(&mut v, (20, 30)));
        assert_eq!(2, v.len());

        assert_eq!(8, update_segments(&mut v, (12, 20)));

        assert_eq!(v, vec![(0, 10), (12, 30)]);
    }

    #[test]
    fn test6() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (0, 10)));
        assert_eq!(10, update_segments(&mut v, (0, 20)));

        assert_eq!(v, vec![(0, 20)]);
    }

    #[test]
    fn test7() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (10, 20)));
        assert_eq!(5, update_segments(&mut v, (0, 5)));

        assert_eq!(v, vec![(0, 5), (10, 20)]);
    }

    #[test]
    fn test8() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (0, 10)));
        assert_eq!(10, update_segments(&mut v, (20, 30)));
        assert_eq!(5, update_segments(&mut v, (10, 15)));

        assert_eq!(v, vec![(0, 15), (20, 30)]);
    }

    #[test]
    fn test9() {
        let mut v = vec![];

        assert_eq!(30, update_segments(&mut v, (0, 30)));
        assert_eq!(0, update_segments(&mut v, (10, 20)));

        assert_eq!(v, vec![(0, 30)]);
    }

    //these are the weird cases where a segment larger than the previous ones comes in

    #[test]
    fn test10() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (0, 10)));
        assert_eq!(10, update_segments(&mut v, (0, 20)));
        assert_eq!(v, vec![(0, 20)]);

        assert_eq!(0, update_segments(&mut v, (10, 15)));

        assert_eq!(v, vec![(0, 20)]);
    }

    #[test]
    fn test11() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (0, 10)));
        assert_eq!(10, update_segments(&mut v, (20, 30)));
        assert_eq!(10, update_segments(&mut v, (40, 50)));

        assert_eq!(v, vec![(0, 10), (20, 30), (40, 50)]);

        assert_eq!(20, update_segments(&mut v, (5, 45)));

        assert_eq!(v, vec![(0, 50)]);
    }

    #[test]
    fn test12() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (0, 10)));
        assert_eq!(10, update_segments(&mut v, (20, 30)));

        assert_eq!(v, vec![(0, 10), (20, 30)]);

        assert_eq!(25, update_segments(&mut v, (5, 45)));

        assert_eq!(v, vec![(0, 45)]);
    }

    #[test]
    fn test13() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (10, 20)));
        assert_eq!(15, update_segments(&mut v, (5, 30)));

        assert_eq!(v, vec![(5, 30)]);
    }

    #[test]
    fn test14() {
        let mut v = vec![];

        assert_eq!(10, update_segments(&mut v, (0, 10)));
        assert_eq!(10, update_segments(&mut v, (20, 30)));
        assert_eq!(10, update_segments(&mut v, (15, 35)));

        assert_eq!(v, vec![(0, 10), (15, 35)]);
    }
    #[test]
    #[should_panic]
    fn test_invalid_segment() {
        let mut v = vec![];

        update_segments(&mut v, (10, 10));
    }
}
