namespace TestGrains
{
	public class TestModel
	{
		public string Greeting { get; set; }

		public override string ToString() => Greeting;
	}
}