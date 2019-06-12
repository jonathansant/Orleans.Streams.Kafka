using Orleans;
using System.Threading.Tasks;

namespace TestGrains
{
	public interface ITestGrain : IGrainWithStringKey
	{
		Task<string> GetThePhrase();
	}
}