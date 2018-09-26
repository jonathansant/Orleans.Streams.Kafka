using System.Collections.Generic;

namespace Orleans.Streams.Kafka.Utils
{
	internal static class DictionaryExtensions
	{
		public static bool TryAdd(this IDictionary<string, object> dict, string key, object value)
		{
			if (dict.ContainsKey(key) || value == null)
				return false;

			dict.Add(key, value);
			return true;
		}
	}
}
