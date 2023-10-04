using Orleans.Concurrency;
using Orleans.Streams.Kafka.E2E.Extensions;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Streaming;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public interface IMultiStreamGrain : IBaseTestGrain
	{
		Task<(TestResult StreamResult1, TestResult StreamResult2)> Fire();
	}

	[Reentrant]
	public class MultiStreamGrain : BaseTestGrain, IMultiStreamGrain
	{
		private IAsyncStream<TestModel> _stream;
		private TestModel _model;
		private TaskCompletionSource<TestResult> _completion;
		private TaskCompletionSource<TestResult> _completion2;
		private IAsyncStream<TestModel> _stream2;

		public override async Task OnActivateAsync(CancellationToken cancellationToken)
		{
			var provider = this.GetStreamProvider(Consts.KafkaStreamProvider);
			_stream = provider.GetStream<TestModel>(Consts.StreamNamespace, Consts.StreamId);
			_stream2 = provider.GetStream<TestModel>(Consts.StreamNamespace2, Consts.StreamId2);

			_model = TestModel.Random();
			_completion = new TaskCompletionSource<TestResult>();
			_completion2 = new TaskCompletionSource<TestResult>();

			await _stream.QuickSubscribe((actual, token) =>
			{
				if (actual.IsLastMessage)
				{
					_completion.SetResult(new TestResult
					{
						Actual = actual,
						Expected = _model
					});
				}

				return Task.CompletedTask;
			});

			await _stream2.QuickSubscribe((actual, token) =>
			{
				if (actual.IsLastMessage)
				{
					_completion2.SetResult(new TestResult
					{
						Actual = actual,
						Expected = _model
					});
				}

				return Task.CompletedTask;
			});
		}

		public async Task<(TestResult StreamResult1, TestResult StreamResult2)> Fire()
		{
			var results = await Task.WhenAll(_completion.Task, _completion2.Task);
			return (results[0], results[1]);
		}
	}
}
