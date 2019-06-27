// ------------------------------------------------------------------------------
// <auto-generated>
//    Generated by avrogen, version 1.7.7.5
//    Changes to this file may cause incorrect behavior and will be lost if code
//    is regenerated
// </auto-generated>
// ------------------------------------------------------------------------------
namespace ConfluentSample
{
	using global::Avro;
	using global::Avro.Specific;

	public partial class AMessage : ISpecificRecord
	{
		public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"AMessage\",\"namespace\":\"ConfluentSample\",\"fields\":[{\"name" +
				"\":\"noOfHeads\",\"type\":\"int\"},{\"name\":\"id\",\"type\":\"int\"}]}");
		private int _noOfHeads;
		private int _id;
		public virtual Schema Schema
		{
			get
			{
				return AMessage._SCHEMA;
			}
		}
		public int noOfHeads
		{
			get
			{
				return this._noOfHeads;
			}
			set
			{
				this._noOfHeads = value;
			}
		}
		public int id
		{
			get
			{
				return this._id;
			}
			set
			{
				this._id = value;
			}
		}
		public virtual object Get(int fieldPos)
		{
			switch (fieldPos)
			{
				case 0: return this.noOfHeads;
				case 1: return this.id;
				default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
			};
		}
		public virtual void Put(int fieldPos, object fieldValue)
		{
			switch (fieldPos)
			{
				case 0: this.noOfHeads = (System.Int32)fieldValue; break;
				case 1: this.id = (System.Int32)fieldValue; break;
				default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
			};
		}
	}
}
