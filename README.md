Entity:
{
	public class Product : IEntity
	{
		public Guid Id { get; set; }

		public string ProductName { get; set; }

		public string BarCode { get; set; }

		public string ProductCode { get; set; }

		public DateTime? CreatedDate { get; set; }
	}
}

Repository:
{
	[Configuration(TableName = "Products")]
    public class ProductRepository : SqlServerRepository<Product>
    {
        public ProductRepository(SqlConnection connection):base(connection)
        {

        }
    }
}

Run Code:
{
	using Persistence.SqlServer.Test;
	var products = new List<Product>();
	for (var i = 0; i < 100000; i++)
	{
		products.Add(new Product()
		{
			Id = Guid.NewGuid(),
			BarCode = i + " BarCode",
			ProductCode = i + " ProductCode",
			ProductName = i + " ProductName",
			CreatedDate = DateTime.Now,
		});
	}




	using(var connection = new  SqlConnection("Data Source=localhost\\SQLEXPRESS01;Database=TestDB;Trusted_Connection=True;;MultipleActiveResultSets=True;"))
	{
		var productRep = new ProductRepository(connection);

		var stopWatch = new Stopwatch();
		stopWatch.Start();
		await productRep.BulkInsertAsync(products).ConfigureAwait(false);
		stopWatch.Stop();
		Console.WriteLine(@$"Bulk Insert Product: {products.Count()} - {stopWatch.ElapsedMilliseconds}ms" );



		stopWatch.Start();
		await productRep.BulkUpdateAsync(products, x => x.ProductName, x => x.ProductCode, x => x.BarCode).ConfigureAwait(false);
		stopWatch.Stop();


		Console.WriteLine(@$"Bulk Update Product: {products.Count()} - {stopWatch.ElapsedMilliseconds}ms");

		stopWatch.Start();
		await productRep.BeginBulkAsync().ConfigureAwait(false);
		for (var i = 0; i < 100000; i++)
		{
			var product = products[i];
			if (i % 2 == 0)
			{
				product.ProductName += $" {i} % 2 = 0";
				productRep.Update(product, x => x.ProductName);
			} else
			{
				product.ProductCode += $" {i} % 2 != 0";
				productRep.Update(product, x => x.ProductCode);
			}
		   
		}
		await productRep.EndBulkAsync().ConfigureAwait(false);
		stopWatch.Stop();

		Console.WriteLine(@$"Complex Bulk Update Product: {products.Count()} - {stopWatch.ElapsedMilliseconds}ms");
	}

}