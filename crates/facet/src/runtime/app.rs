//! An application: the projections a runtime runs.

use std::sync::Arc;

use super::projection::Projection;

/// The projections a [`super::Runtime`] runs.
#[derive(Default, Clone)]
pub struct App {
    projections: Vec<Arc<Projection>>,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.projections.iter().map(|p| p.name()))
            .finish()
    }
}

impl App {
    /// An empty application.
    pub fn new() -> App {
        App::default()
    }

    /// Adds a projection.
    pub fn projection(mut self, p: Projection) -> App {
        self.projections.push(Arc::new(p));
        self
    }

    /// The projections, in registration order.
    pub fn projections(&self) -> &[Arc<Projection>] {
        &self.projections
    }

    /// A projection by name.
    pub fn get(&self, name: &str) -> Option<&Arc<Projection>> {
        self.projections.iter().find(|p| p.name() == name)
    }

    /// Checks that no two projections share a name.
    pub fn validate(&self) -> Result<(), crate::Error> {
        let mut seen = std::collections::BTreeSet::new();
        for p in &self.projections {
            if !seen.insert(p.name()) {
                return Err(crate::Error::Declaration(format!(
                    "two projections named {}",
                    p.name()
                )));
            }
        }
        Ok(())
    }
}
