namespace TaxiPipeline.Domain.Entities;

public class TripRecord
{
    public int SourceLineNumber { get; set; }
    public string? VendorId { get; set; }
    public string? PickupDatetime { get; set; }
    public string? DropoffDatetime { get; set; }
    public string? PassengerCount { get; set; }
    public string? TripDistance { get; set; }
    public string? RateCode { get; set; }
    public string? StoreAndFwdFlag { get; set; }
    public string? PickupLocationId { get; set; }
    public string? DropoffLocationId { get; set; }
    public string? PaymentType { get; set; }
    public string? FareAmount { get; set; }
    public string? Extra { get; set; }
    public string? MtaTax { get; set; }
    public string? TipAmount { get; set; }
    public string? TollsAmount { get; set; }
    public string? ImprovementSurcharge { get; set; }
    public string? TotalAmount { get; set; }
    public string? CongestionSurcharge { get; set; }
    public string? AirportFee { get; set; }
}
