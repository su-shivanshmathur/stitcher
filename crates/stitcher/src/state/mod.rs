//! `state.yaml` compiles once at boot into a [`config::Program`], which drives
//! [`interp::ConfigProcessor`] over the dynamic [`crate::merge::MergeValue`] tree.

pub mod config;
pub mod interp;

pub use interp::ConfigProcessor;
