using System.Collections.Generic;

namespace Orleans.Streams.Kafka.Utils
{
	internal static class DictionaryExtensions
	{
		public static bool TryAdd(this IDictionary<string, string> dict, string key, string value)
		{
			if (dict.ContainsKey(key) || value == null)
				return false;

			dict.Add(key, value);
			return true;
		}
	}
}
