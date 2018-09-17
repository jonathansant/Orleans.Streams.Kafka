using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public class BaseTestGrain : Grain, IBaseTestGrain
	{
		public Task WakeUp()
			=> Task.CompletedTask;
	}

	public interface IBaseTestGrain : IGrainWithStringKey
	{
		Task WakeUp();
	}
}
