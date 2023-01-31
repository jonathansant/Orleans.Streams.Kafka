using Newtonsoft.Json.Linq;
using Orleans.Concurrency;
using Orleans.Streams.Kafka.E2E.Extensions;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public interface IRoundTripDynamicModelGrain : IBaseTestGrain
	{
		Task<TestResult> Fire();
	}

	[Reentrant]
	public class RoundTripDynamicModelGrain : BaseTestGrain, IRoundTripDynamicModelGrain
	{
		private IAsyncStream<JObject> _stream;
		private TestModel _model;
		private TaskCompletionSource<TestResult> _completion;

		public override async Task OnActivateAsync(CancellationToken _)
		{
			var provider = this.GetStreamProvider(Consts.KafkaStreamProvider);
			_stream = provider.GetStream<JObject>(Consts.StreamNamespace, Consts.StreamId3);

			_model = TestModel.Random();
			_completion = new TaskCompletionSource<TestResult>();

			await _stream.QuickSubscribe((actual, token) =>
			{
				_completion.SetResult(new TestResult
				{
					Actual = actual.ToObject<TestModel>(),
					Expected = _model
				});

				return Task.CompletedTask;
			});

			await Task.Delay(5000);
		}

		public async Task<TestResult> Fire()
		{
			await _stream.OnNextAsync(JObject.FromObject(_model));
			await Task.WhenAny(_completion.Task, Task.Delay(1000));

			return _completion.Task.IsCompleted
				? _completion.Task.Result
				: null;
		}
	}
}
