using Orleans.Concurrency;
using Orleans.Streams.Kafka.E2E.Extensions;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public interface IRoundTripGrain : IBaseTestGrain
	{
		Task<TestResult> Fire();
	}

	[Reentrant]
	public class RoundTripGrain : BaseTestGrain, IRoundTripGrain
	{
		private IAsyncStream<TestModel> _stream;
		private TestModel _model;
		private TaskCompletionSource<TestResult> _completion;

		public override async Task OnActivateAsync(CancellationToken _)
		{
			var provider = this.GetStreamProvider(Consts.KafkaStreamProvider);
			_stream = provider.GetStream<TestModel>(Consts.StreamNamespace, Consts.StreamId);

			_model = TestModel.Random();
			_completion = new TaskCompletionSource<TestResult>();

			await _stream.SubscribeAsync((actual, token) =>
			{
				_completion.SetResult(new TestResult
				{
					Actual = actual,
					Expected = _model
				});

				return Task.CompletedTask;
			});

			await Task.Delay(5);
		}

		public async Task<TestResult> Fire()
		{
			await _stream.OnNextAsync(_model);
			await Task.WhenAny(_completion.Task, Task.Delay(6000));

			return _completion.Task.IsCompleted
				? _completion.Task.Result
				: null;
		}
	}
}
