//! Deterministic conditional acquisition. A recipe is not an executable query.
use super::*;
use crate::investigator::{DirectionV1, FocusV1, PreparedInvestigationV1};
use decant_backend::model::two_stage::RecipeV1;

#[derive(Clone, Debug, Serialize)]
pub struct StepV1 {
    pub signal: &'static str,
    pub decision: &'static str,
}
pub struct AdaptiveCaptureV1 {
    pub bundle: ReplayBundleV1,
    pub steps: Vec<StepV1>,
}
pub fn focus(recipe: RecipeV1) -> Result<FocusV1, E> {
    match recipe {
        RecipeV1::Latency => Ok(FocusV1::Latency),
        RecipeV1::Errors => Ok(FocusV1::Errors),
        _ => Err(E::InvalidInput),
    }
}
/// Checks all potentially reused query/policy identities before any acquisition.
pub fn validate_seed(
    config: &InvestigationConfigV1,
    seed: &CapturedInvestigationV1,
) -> Result<(), E> {
    let (m, t, l) = config.plans()?;
    let (sm, st, sl) = seed.config.plans()?;
    if m.id() != sm.id()
        || t.as_ref()
            .zip(st.as_ref())
            .is_some_and(|(a, b)| a.id() != b.id())
        || l.as_ref()
            .zip(sl.as_ref())
            .is_some_and(|(a, b)| a.id() != b.id())
    {
        return Err(E::IdentityMismatch);
    }
    Ok(())
}
pub async fn acquire(
    config: InvestigationConfigV1,
    seed: Option<CapturedInvestigationV1>,
    recipe: RecipeV1,
    backend: &mut GrafanaV1,
    mut publish: impl FnMut(&ReplayBundleV1) -> Result<(), E>,
) -> Result<AdaptiveCaptureV1, E> {
    let selected = focus(recipe)?;
    let (m, t, l) = config.plans()?;
    if let Some(s) = &seed {
        validate_seed(&config, s)?;
    }
    let mut steps = Vec::new();
    let mut working = config.clone();
    working.tempo = None;
    working.loki = None;
    let reads = if let Some(s) = &seed {
        steps.push(StepV1 {
            signal: "metrics",
            decision: "reused_same_cut",
        });
        s.reads
            .iter()
            .filter(|r| r.plan_id() == m.id())
            .cloned()
            .collect()
    } else {
        steps.push(StepV1 {
            signal: "metrics",
            decision: "acquired",
        });
        backend
            .acquire_metrics(&m)
            .await?
            .iter()
            .map(CaptureV1::as_read)
            .collect()
    };
    let mut captured = CapturedInvestigationV1::from_reads(working, reads)?;
    let mut bundle = captured.bundle()?;
    publish(&bundle)?; // Valid evidence survives failed later acquisition/model calls.
    let direction = PreparedInvestigationV1::new(bundle.report())?.signal_direction(selected);
    let expand = matches!(
        direction,
        DirectionV1::Increase | DirectionV1::Unknown | DirectionV1::Mixed
    );
    for signal in ["tempo", "loki"] {
        let id = if signal == "tempo" {
            t.as_ref().map(|p| p.id())
        } else {
            l.as_ref().map(|p| p.id())
        };
        let Some(id) = id else {
            steps.push(StepV1 {
                signal,
                decision: "not_enabled",
            });
            continue;
        };
        let existing: Vec<_> = seed
            .as_ref()
            .map(|s| {
                s.reads
                    .iter()
                    .filter(|r| r.plan_id() == id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        if !expand && existing.is_empty() {
            steps.push(StepV1 {
                signal,
                decision: "skipped_no_observed_increase",
            });
            continue;
        }
        let extra = if !existing.is_empty() {
            steps.push(StepV1 {
                signal,
                decision: "reused_same_cut",
            });
            existing
        } else {
            steps.push(StepV1 {
                signal,
                decision: "acquired_after_metrics",
            });
            if signal == "tempo" {
                backend
                    .acquire_tempo(t.as_ref().ok_or(E::InvalidInput)?)
                    .await?
            } else {
                backend
                    .acquire_loki(l.as_ref().ok_or(E::InvalidInput)?)
                    .await?
            }
        };
        if signal == "tempo" {
            captured.config.tempo = config.tempo.clone();
        } else {
            captured.config.loki = config.loki.clone();
        }
        captured.reads.extend(extra);
        captured = CapturedInvestigationV1::from_reads(captured.config, captured.reads)?;
        bundle = captured.bundle()?;
        publish(&bundle)?;
    }
    Ok(AdaptiveCaptureV1 { bundle, steps })
}
