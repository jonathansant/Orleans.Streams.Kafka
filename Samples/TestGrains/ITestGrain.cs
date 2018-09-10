using System.Threading.Tasks;
using Orleans;

namespace TestGrains
{
	public interface ITestGrain : IGrainWithStringKey
	{
		Task<string> GetThePhrase();
	}
}