namespace TaxiPipeline.Domain.Enums;

public enum PipelineStep
{
    StartBatch,
    ReadFile,
    InsertLanding,
    CleanData,
    RejectInvalid,
    Deduplicate,
    LoadCore,
    FinishBatch
}
