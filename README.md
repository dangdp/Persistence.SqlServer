Entity:

public class Product : IEntity
{
    public Guid Id { get; set; }

    public string ProductName { get; set; }

    public string BarCode { get; set; }

    public string ProductCode { get; set; }

    public DateTime? CreatedDate { get; set; }
}