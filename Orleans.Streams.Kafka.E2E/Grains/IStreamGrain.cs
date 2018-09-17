using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public interface IStreamGrain : IBaseTestGrain
	{
		Task<string> SaySomething(string something);
		Task<TestModel> WhatDidIGet();
	}
}