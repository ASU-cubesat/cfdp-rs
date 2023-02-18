use std::cmp::Ordering;

// update the contiguous segment list with the new segment, filling any gap (i.e. unifying segments) if required
pub fn update_segments(v: &mut Vec<(u64, u64)>, seg: (u64, u64)) {
    assert!(seg.0 < seg.1, "invalid segment");

    let len = v.len();
    if len == 0 {
        v.push(seg);
        return;
    }

    let last = &mut v[len - 1];
    match last.1.cmp(&seg.0) {
        Ordering::Equal => {
            //most probable case - the data fits nicely at the end
            last.1 = seg.1
        }
        Ordering::Less => {
            //there was just a data loss and now a new segment comes at the end of the list
            v.push(seg)
        }
        Ordering::Greater => {
            // data fills some gap in the middle, use binary search to find where it should go
            match v.binary_search_by(|x| x.0.cmp(&seg.0)) {
                Ok(k) => {
                    // the  segment starts at the same offset with the segment v[k]
                    v[k].1 = u64::max(v[k].1, seg.1);
                    merge(v, k, seg.1);
                }
                Err(k) => {
                    if k == 0 {
                        //new segment comes right at the beginning of the list
                        if seg.1 >= v[0].0 {
                            v[0].0 = seg.0;
                            merge(v, 0, seg.1);
                        } else {
                            v.insert(0, seg);
                        }
                    } else {
                        //here we know there is an element to the left
                        if v[k - 1].1 >= seg.0 {
                            //overlaps with the left
                            if v[k - 1].1 < seg.1 {
                                v[k - 1].1 = seg.1;
                                merge(v, k - 1, seg.1);
                            } //else seg is completely embedded in v[k-1]
                        } else {
                            //does not overlap with left
                            //we know there is an element to the right (i.e. k < len),
                            // otherwise this would be the last in the list and we have tested for that
                            // in the "else if last.1 < seg.0" above
                            if seg.1 < v[k].0 {
                                //does not overlap with right either
                                v.insert(k, seg);
                            } else {
                                //overlaps with right, we have to extend the right to the left
                                v[k].0 = seg.0;
                                merge(v, k, seg.1);
                            }
                        }
                    }
                }
            }
        }
    }
}

//merge v[k] with all segments s from its right having s.start <= end
fn merge(v: &mut Vec<(u64, u64)>, k: usize, end: u64) {
    // it is very unlikely this will loop more than once so no need to bother removing in bulk
    while k + 1 < v.len() && v[k + 1].0 <= end {
        v[k].1 = v[k + 1].1;
        v.remove(k + 1);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test1() {
        let mut v = vec![];
        update_segments(&mut v, (0, 10));
        update_segments(&mut v, (10, 20));

        assert_eq!(v, vec![(0, 20)]);
    }

    #[test]
    fn test2() {
        let mut v = vec![];
        update_segments(&mut v, (10, 20));
        update_segments(&mut v, (0, 10));

        assert_eq!(v, vec![(0, 20)]);
    }

    #[test]
    fn test3() {
        let mut v = vec![];

        update_segments(&mut v, (0, 10));
        update_segments(&mut v, (20, 30));
        assert_eq!(2, v.len());

        update_segments(&mut v, (10, 20));

        assert_eq!(v, vec![(0, 30)]);
    }

    #[test]
    fn test4() {
        let mut v = vec![];

        update_segments(&mut v, (0, 10));
        update_segments(&mut v, (20, 30));
        assert_eq!(2, v.len());

        update_segments(&mut v, (12, 15));

        assert_eq!(v, vec![(0, 10), (12, 15), (20, 30)]);
    }

    #[test]
    fn test5() {
        let mut v = vec![];

        update_segments(&mut v, (0, 10));
        update_segments(&mut v, (20, 30));
        assert_eq!(2, v.len());

        update_segments(&mut v, (12, 20));

        assert_eq!(v, vec![(0, 10), (12, 30)]);
    }

    #[test]
    fn test6() {
        let mut v = vec![];

        update_segments(&mut v, (0, 10));
        update_segments(&mut v, (0, 20));

        assert_eq!(v, vec![(0, 20)]);
    }

    #[test]
    fn test7() {
        let mut v = vec![];

        update_segments(&mut v, (10, 20));
        update_segments(&mut v, (0, 5));

        assert_eq!(v, vec![(0, 5), (10, 20)]);
    }

    #[test]
    fn test8() {
        let mut v = vec![];

        update_segments(&mut v, (0, 10));
        update_segments(&mut v, (20, 30));
        update_segments(&mut v, (10, 15));

        assert_eq!(v, vec![(0, 15), (20, 30)]);
    }

    #[test]
    fn test9() {
        let mut v = vec![];

        update_segments(&mut v, (0, 30));
        update_segments(&mut v, (10, 20));

        assert_eq!(v, vec![(0, 30)]);
    }

    //these are the weird cases where a segment larger than the previous ones comes in

    #[test]
    fn test10() {
        let mut v = vec![];

        update_segments(&mut v, (0, 10));
        update_segments(&mut v, (0, 20));
        assert_eq!(v, vec![(0, 20)]);

        update_segments(&mut v, (10, 15));

        assert_eq!(v, vec![(0, 20)]);
    }

    #[test]
    fn test11() {
        let mut v = vec![];

        update_segments(&mut v, (0, 10));
        update_segments(&mut v, (20, 30));
        update_segments(&mut v, (40, 50));

        assert_eq!(v, vec![(0, 10), (20, 30), (40, 50)]);

        update_segments(&mut v, (5, 45));

        assert_eq!(v, vec![(0, 50)]);
    }

    #[test]
    #[should_panic]
    fn test_invalid_segment() {
        let mut v = vec![];

        update_segments(&mut v, (10, 10));
    }
}
