// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Frontier state for each arrangement.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};
use timely::progress::Timestamp;

use expr::GlobalId;

/// A map from global identifiers to arrangement frontier state.
pub struct ArrangementFrontiers<T: Timestamp> {
    index: HashMap<GlobalId, Frontiers<T>>,
}

impl<T: Timestamp> Default for ArrangementFrontiers<T> {
    fn default() -> Self {
        Self {
            index: HashMap::new(),
        }
    }
}

impl<T: Timestamp> ArrangementFrontiers<T> {
    pub fn get(&self, id: &GlobalId) -> Option<&Frontiers<T>> {
        self.index.get(id)
    }
    pub fn get_mut(&mut self, id: &GlobalId) -> Option<&mut Frontiers<T>> {
        self.index.get_mut(id)
    }
    pub fn contains_key(&self, id: GlobalId) -> bool {
        self.index.contains_key(&id)
    }
    pub fn insert(&mut self, id: GlobalId, state: Frontiers<T>) -> Option<Frontiers<T>> {
        self.index.insert(id, state)
    }
    pub fn remove(&mut self, id: &GlobalId) -> Option<Frontiers<T>> {
        self.index.remove(id)
    }

    /// The upper frontier of a maintained index, if it exists.
    pub fn upper_of(&self, name: &GlobalId) -> Option<AntichainRef<T>> {
        if let Some(index_state) = self.get(name) {
            Some(index_state.upper.frontier())
        } else {
            None
        }
    }

    /// The since frontier of a maintained index, if it exists.
    pub fn since_of(&self, name: &GlobalId) -> Option<Antichain<T>> {
        if let Some(index_state) = self.get(name) {
            Some(index_state.since.lock().unwrap().frontier().to_owned())
        } else {
            None
        }
    }

    /// Reports the greatest frontier less than all identified `upper` frontiers.
    pub fn greatest_open_upper<I>(&self, identifiers: I) -> Antichain<T>
    where
        I: IntoIterator<Item = GlobalId>,
        T: Lattice,
    {
        // Form lower bound on available times
        let mut min_upper = Antichain::new();
        for id in identifiers {
            // To track the meet of `upper` we just extend with the upper frontier.
            // This was almost `meet_assign` but our uppers are `MutableAntichain`s.
            min_upper.extend(self.upper_of(&id).unwrap().iter().cloned());
        }
        min_upper
    }

    /// Reports the minimal frontier greater than all identified `since` frontiers.
    pub fn least_valid_since<I>(&self, identifiers: I) -> Antichain<T>
    where
        I: IntoIterator<Item = GlobalId>,
        T: Lattice,
    {
        let mut max_since = Antichain::from_elem(T::minimum());
        for id in identifiers {
            // TODO: We could avoid repeated allocation by swapping two buffers.
            max_since.join_assign(&self.since_of(&id).expect("Since missing at coordinator"));
        }
        max_since
    }
}

pub struct Frontiers<T: Timestamp> {
    /// The most recent frontier for new data.
    /// All further changes will be in advance of this bound.
    pub upper: MutableAntichain<T>,
    /// The compaction frontier.
    /// All peeks in advance of this frontier will be correct,
    /// but peeks not in advance of this frontier may not be.
    pub since: Arc<Mutex<MutableAntichain<T>>>,
    /// Compaction delay.
    ///
    /// This timestamp drives the advancement of the since frontier as a
    /// function of the upper frontier, trailing it by exactly this much.
    pub compaction_window_ms: Option<T>,
}

impl<T: Timestamp + Copy> Frontiers<T> {
    /// Creates an empty index state from a number of workers.
    pub fn new(workers: usize, compaction_window_ms: Option<T>) -> (Self, MutableAntichainRc<T>) {
        let mut upper = MutableAntichain::new();
        upper.update_iter(Some((T::minimum(), workers as i64)));
        let (initial_since, since) = MutableAntichainRc::make_from(T::minimum());
        (
            Self {
                upper,
                since,
                compaction_window_ms,
            },
            initial_since,
        )
    }

    /// Sets the latency behind the collection frontier at which compaction occurs.
    pub fn set_compaction_window_ms(&mut self, window_ms: Option<T>) {
        self.compaction_window_ms = window_ms;
    }

    pub fn since_handle(&self, values: Vec<T>) -> MutableAntichainRc<T> {
        let wrapper = Arc::clone(&self.since);
        let changes = wrapper
            .lock()
            .unwrap()
            .update_iter(values.iter().map(|x| (*x, 1)));
        // The since must never retreat when new timestamps are added.
        assert!(changes.next().is_none());
        let drop_changes = Arc::clone(&self.drop_changes);
        MutableAntichainRc {
            values,
            wrapper,
            drop_changes,
        }
    }
}

pub struct MutableAntichainRc<T: Timestamp + Copy> {
    values: Vec<T>,
    // We have to use Arc here because the Coordinator is sent one time to a tokio
    // task which requires everything to be Send, and Rc is !Send.
    wrapper: Arc<Mutex<MutableAntichain<T>>>,
    drop_changed: Arc<Mutex<Antichain<T>>>,
}

impl<T: Timestamp + Copy> MutableAntichainRc<T> {
    /// Allocates a new handle from an antichain.
    pub fn make_from(
        value: T,
        drop_changed: Arc<Mutex<Antichain<T>>>,
    ) -> (Self, Arc<Mutex<MutableAntichain<T>>>) {
        let wrapped = Arc::new(Mutex::new(MutableAntichain::new_bottom(value.clone())));

        let handle = MutableAntichainRc {
            values: vec![value],
            wrapper: Arc::clone(&wrapped),
            drop_changed,
        };

        (handle, wrapped)
    }
}

impl<T: Timestamp + Copy> Clone for MutableAntichainRc<T> {
    fn clone(&self) -> Self {
        // Increase ref count for this value.
        self.wrapper
            .lock()
            .unwrap()
            .update_iter(self.values.iter().map(|x| (*x, 1)));
        MutableAntichainRc {
            values: self.values.clone(),
            wrapper: Arc::clone(&self.wrapper),
        }
    }
}

impl<T: Timestamp + Copy> Drop for MutableAntichainRc<T> {
    fn drop(&mut self) {
        let inner = self.wrapper.lock().unwrap();
        let changes = inner.update_iter(self.values.iter().map(|x| (*x, -1)));
        if changes.next().is_some() {
            self.drop_changed.lock().unwrap() = inner.frontier().to_owned();
        }
    }
}
