using Orleans.Concurrency;
using Orleans.Streams.Kafka.E2E.Extensions;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public interface IStreamGrainV2 : IBaseTestGrain
	{
		Task<TestResult> Fire();
	}

	[Reentrant]
	public class RoundTripGrain : BaseTestGrain, IStreamGrainV2
	{
		private IAsyncStream<TestModel> _stream;
		private TestModel _model;
		private TaskCompletionSource<TestResult> _completion;

		public override async Task OnActivateAsync()
		{
			var provider = GetStreamProvider(Consts.KafkaStreamProvider);
			_stream = provider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);

			_model = TestModel.Random();
			_completion = new TaskCompletionSource<TestResult>();

			await _stream.QuickSubscribe((actual, token) =>
			{
				_completion.SetResult(new TestResult
				{
					Actual = actual,
					Expected = _model
				});

				return Task.CompletedTask;
			});
		}

		public async Task<TestResult> Fire()
		{
			await _stream.OnNextAsync(_model);
			await Task.WhenAny(_completion.Task, Task.Delay(10000));

			return _completion.Task.IsCompleted 
				? _completion.Task.Result 
				: null;
		}
	}
}
