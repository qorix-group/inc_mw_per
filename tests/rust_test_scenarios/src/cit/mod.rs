use test_scenarios_rust::scenario::{ScenarioGroup, ScenarioGroupImpl};

use crate::cit::supported_datatypes::supported_datatypes_group;

mod supported_datatypes;

pub fn cit_scenario_group() -> Box<dyn ScenarioGroup> {
    Box::new(ScenarioGroupImpl::new(
        "cit",
        vec![],
        vec![supported_datatypes_group()],
    ))
}
