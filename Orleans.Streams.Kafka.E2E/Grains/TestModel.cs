using System;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	[GenerateSerializer]
	public class TestModel
	{
		private static Random Rand = new Random();

		[Id(0)]
		public int NumberOfLegs { get; set; }
		
		[Id(1)]
		public int NumberOfHeads { get; set; }
		
		[Id(2)]
		public bool IsLastMessage { get; set; }

		public override bool Equals(object obj)
		{
			var mod = (TestModel)obj;
			return mod.NumberOfHeads == NumberOfHeads && mod.NumberOfLegs == NumberOfLegs;
		}

		public static TestModel Random()
			=> new TestModel
			{
				NumberOfHeads = Rand.Next(1000),
				NumberOfLegs = Rand.Next(1000)
			};
	}

	[GenerateSerializer]
	public class TestResult
	{
		[Id(0)]
		public TestModel Expected { get; set; }
		
		[Id(1)]
		public TestModel Actual { get; set; }
	}
}