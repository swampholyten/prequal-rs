//! Server-side components: the backend [`replica`] (work + probe endpoints)
//! and the optional CPU-burning [`antagonist`] it can host.

pub mod antagonist;
pub mod replica;
