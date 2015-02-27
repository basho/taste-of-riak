namespace Ch03_Msgy_Schema.Repositories
{
    using System;
    using Models;
    using RiakClient;
    using RiakClient.Models;

    public abstract class Repository<TModel> : IRepository<TModel> where TModel : IModel
    {
        private IRiakClient client;

        public Repository(IRiakClient client)
        {
            if (client == null)
            {
                throw new ArgumentNullException("client");
            }
            this.client = client;
        }

        public virtual TModel Get(string key, bool notFoundOK = false)
        {
            var riakObjectId = new RiakObjectId(BucketName, key);
            RiakResult<RiakObject> result = client.Get(riakObjectId);
            CheckResult(result, notFoundOK);
            RiakObject value = result.Value;
            if (notFoundOK && value == null)
            {
                return default(TModel);
            }
            else
            {
                return value.GetObject<TModel>();
            }
        }

        public virtual string Save(TModel model)
        {
            var riakObjectId = new RiakObjectId(BucketName, model.ID);
            var riakObject = new RiakObject(riakObjectId, model);
            RiakResult<RiakObject> result = client.Put(riakObject);
            CheckResult(result);
            RiakObject value = result.Value;
            return value.Key;
        }

        protected abstract string BucketName { get; }

        private void CheckResult(RiakResult result, bool notFoundOK = false)
        {
            if (!result.IsSuccess)
            {
                if (notFoundOK && result.ResultCode == ResultCode.NotFound)
                {
                    // No-op since not_found response is OK
                }
                else
                {
                    throw new ApplicationException(string.Format("Riak failure: {0}", result.ErrorMessage));
                }
            }
        }
    }
}
