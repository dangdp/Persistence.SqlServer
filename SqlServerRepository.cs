﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Persistence.SqlServer
{
    public class SqlServerRepository<TEntity> where TEntity : IEntity
    {
        private PersistenceContext context;
        public SqlServerRepository(SqlConnection connection) {
            context = new PersistenceContext(connection);
            context.InitializeConfiguration(this.GetType());
        }

        public SqlServerRepository(PersistenceContext context)
        {
            this.context = context;
            context.InitializeConfiguration(this.GetType());
        }

        private Dictionary<string, object> _mappingScope = new Dictionary<string, object>();
        private static object IsLocked = new object();


        private Hashtable _mappingEntities = new Hashtable();

        public async Task<TEntity> GeTEntityByIdAsync(Guid id)
        {
            if (_mappingEntities[id] != null)
            {
                return (TEntity)_mappingEntities[id];
            }

            var entity = await this.FirstOrDefaultAsync(m => m.Id == id).ConfigureAwait(false);

            _mappingEntities[id] = entity;

            return entity;
        }

        private bool isBulk = false;

        private List<TEntity> insertEntities;
        private Dictionary<string, UpdatEntities<TEntity>> updateEntities;
        protected List<TEntity> insertedEntities = new List<TEntity>();
        protected List<TEntity> updatedEntities = new List<TEntity>();
        protected List<TEntity> removedEntities = new List<TEntity>();

        class UpdatEntities<TEntity>
        {
            public List<TEntity> UpdatedEntities { get; set; } = new List<TEntity>();

            public Expression<Func<TEntity, object>>[] UpdatedProperties { get; set; }
        }

        public async Task BeginBulkAsync()
        {
            this.isBulk = true;

            this.insertEntities = new List<TEntity>();

            this.updateEntities = new Dictionary<string, UpdatEntities<TEntity>>();

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public async Task<TEntity> GetEntityByIdAsync(Guid id)
        {
            if (_mappingEntities[id] != null)
            {
                return (TEntity)_mappingEntities[id];
            }

            var entity = await this.FirstOrDefaultAsync(m => m.Id == id).ConfigureAwait(false);

            _mappingEntities[id] = entity;

            return entity;
        }


        public async Task EndBulkAsync()
        {
            if (this.isBulk)
            {
                this.isBulk = false;
                await this.BulkInsertAsync(this.insertEntities).ConfigureAwait(false);
                foreach (var group in this.updateEntities)
                {
                    var target = group.Value;
                    await this.BulkUpdateAsync(target.UpdatedEntities, target.UpdatedProperties).ConfigureAwait(false);
                }

            }
        }

        public virtual void Insert(TEntity entity)
        {
            if (this.isBulk)
            {
                this.insertEntities.Add(entity);
                return;
            }
            this.context.Insert(entity, this.GetType());
            insertedEntities.Add(entity);
        }

        public virtual async Task BulkInsertAsync(ICollection<TEntity> entities)
        {
            if (entities == null || entities.Count == 0)
            {
                return;
            }
            var listEntities = entities.ToList();
            await this.context.BulkInsertAsync<TEntity>(listEntities, this.GetType()).ConfigureAwait(false);
            insertedEntities.AddRange(entities);
        }

        public virtual async Task BulkUpdateAsync(ICollection<TEntity> entities)
        {
            if (entities == null || entities.Count == 0)
            {
                return;
            }
            await this.context.BulkUpdateAsync<TEntity>(entities.ToList(), this.GetType(), new List<string>().ToArray()).ConfigureAwait(false);
            updatedEntities.AddRange(entities);
        }

        /// <summary>
        /// Recommend to use this
        /// </summary>
        /// <param name="entities"></param>
        /// <param name="updatedProperties"></param>
        /// <returns></returns>
        public virtual async Task BulkUpdateAsync(ICollection<TEntity> entities, params Expression<Func<TEntity, object>>[] updatedProperties)
        {
            if (entities == null || entities.Count == 0)
            {
                return;
            }
            if (updatedProperties.Count() == 0)
            {
                return;
            }
            await this.context.BulkUpdate<TEntity>(entities.ToList(), this.GetType(), updatedProperties).ConfigureAwait(false);
            updatedEntities.AddRange(entities);
        }

        /// <summary>
        /// Not recommend to use this, just for dynamic updating field
        /// </summary>
        /// <param name="entities"></param>
        /// <param name="updatedProperties"></param>
        /// <returns></returns>
        public virtual async Task BulkUpdateAsync(ICollection<TEntity> entities, params string[] updatedProperties)
        {
            var start = DateTime.Now;
            if (entities == null || entities.Count == 0)
            {
                return;
            }
            if (updatedProperties.Length == 0)
            {
                return;
            }
            await this.context.BulkUpdateAsync<TEntity>(entities.ToList(), this.GetType(), updatedProperties).ConfigureAwait(false);
            updatedEntities.AddRange(entities);
        }

        public virtual void Remove(params TEntity[] entity)
        {
            this.context.Remove(this.GetType(), entity);
            removedEntities.AddRange(entity);
        }

        public virtual async Task RemoveAsync(params TEntity[] entity)
        {
            await this.context.RemoveAsync(this.GetType(), entity).ConfigureAwait(false);
            removedEntities.AddRange(entity);
        }

        public virtual void Update(TEntity item, params Expression<Func<TEntity, object>>[] updatedProperties)
        {
            try
            {
                if (this.isBulk)
                {
                    var columnNames = updatedProperties.Select(m => m.GetFieldName()).ToList();
                    var key = string.Join(",", columnNames);
                    if (!this.updateEntities.ContainsKey(key))
                    {
                        this.updateEntities.Add(key, new UpdatEntities<TEntity>()
                        {
                            UpdatedProperties = updatedProperties,
                            UpdatedEntities = new List<TEntity>()
                        });
                    }

                    var updatedTarget = this.updateEntities[key];
                    updatedTarget.UpdatedEntities.Add(item);
                    return;
                }
                this.context.Update(item, this.GetType(), updatedProperties);
                updatedEntities.Add(item);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        public virtual IEnumerable<TEntity> Where(Expression<Func<TEntity, bool>> condition)
        {
            return this.context.Query<TEntity>(condition, this.GetType(), true).ToList();
        }

        public virtual async Task<List<TOut>> WhereAsync<TOut>(Expression<Func<TEntity, bool>> condition)
        {
            var items = await this.context.QueryAsync<TEntity, TOut>(condition, this.GetType(), true).ConfigureAwait(false);

            return items.ToList();
        }

        public async new Task<List<T>> QueryAsync<T>(string sql, object param = null)
        {
            var items = await this.context.QueryAsync<T>(sql, param).ConfigureAwait(false);
            return items.ToList();
        }

        public async Task BeginBulk()
        {
            this.isBulk = true;

            this.insertEntities = new List<TEntity>();

            this.updateEntities = new Dictionary<string, UpdatEntities<TEntity>>();

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public async Task EndBulk()
        {
            if (this.isBulk)
            {
                this.isBulk = false;
                await this.BulkInsertAsync(this.insertEntities).ConfigureAwait(false);
                foreach (var group in this.updateEntities)
                {
                    var target = group.Value;
                    await this.BulkUpdateAsync(target.UpdatedEntities, target.UpdatedProperties).ConfigureAwait(false);
                }
            }
        }

        public virtual async Task<List<TOut>> WhereAsync<TOut>(Expression<Func<TEntity, bool>> condition, Expression<Func<TEntity, object>> column)
        {
            var items = await this.context.QueryAsync<TEntity, TOut>(condition, this.GetType(), true, column).ConfigureAwait(false);

            return items.ToList();
        }

        public virtual async Task<int?> CountAsync(Expression<Func<TEntity, bool>> condition)
        {
            return await this.context.CountAsync<TEntity>(condition, this.GetType(), true).ConfigureAwait(false);
        }

        public virtual async Task<int?> SumAsync(Expression<Func<TEntity, bool>> condition, Expression<Func<TEntity, object>> columnSum)
        {
            return await this.context.SumAsync<TEntity>(condition, columnSum, this.GetType(), true).ConfigureAwait(false);
        }



        public virtual async Task<IEnumerable<TEntity>> WhereAsync(Expression<Func<TEntity, bool>> condition)
        {
            return await this.context.QueryAsync<TEntity>(condition, this.GetType(), true).ConfigureAwait(false);
        }

        public virtual IEnumerable<TEntity> WhereWithoutDelete(Expression<Func<TEntity, bool>> condition)
        {
            return this.context.Query<TEntity>(condition, this.GetType()).ToList();
        }

        public virtual async Task<IEnumerable<TEntity>> WhereWithoutDeleteAsync(Expression<Func<TEntity, bool>> condition)
        {
            return await this.context.QueryAsync<TEntity>(condition, this.GetType()).ConfigureAwait(false);
        }

        public virtual TEntity FirstOrDefault(Expression<Func<TEntity, bool>> condition)
        {
            return this.context.FirstOrDefault<TEntity>(condition, this.GetType(), true);
        }

        public virtual async Task<TEntity> FirstOrDefaultAsync(Expression<Func<TEntity, bool>> condition)
        {
            return await this.context.FirstOrDefaultAsync<TEntity>(condition, this.GetType(), true).ConfigureAwait(false);
        }

        public virtual IEnumerable<TEntity> FirstOrDefaultWithoutDelete(Expression<Func<TEntity, bool>> condition)
        {
            return this.context.Query<TEntity>(condition, this.GetType()).ToList();
        }

        public virtual async Task<IEnumerable<TEntity>> FirstOrDefaultWithoutDeleteAsync(Expression<Func<TEntity, bool>> condition)
        {
            return await this.context.QueryAsync<TEntity>(condition, this.GetType()).ConfigureAwait(false);
        }

        public T FirstOrDefault<T>(string text, object param = null)
        {
            return this.context.FirstOrDefault<T>(text, param);
        }

        public virtual async Task<T> FirstOrDefaultAsync<T>(string text, object param = null)
        {
            return await this.context.FirstOrDefaultAsync<T>(text, param).ConfigureAwait(false);
        }

        public virtual async Task<int> ExecuteAsync(string sql, object param = null)
        {
            return await this.context.ExecuteAsync(sql, param).ConfigureAwait(false);
        }
        public virtual List<T> Query<T>(string sql, object param = null)
        {
            return this.context.Query<T>(sql, param).ToList();
        }

        public virtual Tuple<List<T1>, List<T2>> Query<T1, T2>(string sql, object param = null)
        {
            var result = this.context.Query<T1, T2>(sql, param);

            return new Tuple<List<T1>, List<T2>>(result.Item1.ToList(), result.Item2.ToList());
        }

        public virtual async Task<Tuple<List<T1>, List<T2>>> QueryAsync<T1, T2>(string sql, object param = null)
        {
            var result = await this.context.QueryAsync<T1, T2>(sql, param).ConfigureAwait(false);

            return new Tuple<List<T1>, List<T2>>(result.Item1.ToList(), result.Item2.ToList());
        }

        public virtual IEnumerable<TEntity> GetAll()
        {
            return this.context.Query<TEntity>(null, this.GetType(), true).ToList();
        }

        public virtual async Task<IEnumerable<TEntity>> GetAllAsync()
        {
            return await this.context.QueryAsync<TEntity>(null, this.GetType(), true).ConfigureAwait(false);
        }

        public virtual async Task<T> MaxAsync<T>(Expression<Func<TEntity, bool>> predicate, Expression<Func<TEntity, T>> column)
        {
            return await this.context.MaxAsync<TEntity, T>(predicate, column, this.GetType(), true).ConfigureAwait(false);
        }

        public virtual async Task<T> MinAsync<T>(Expression<Func<TEntity, bool>> predicate, Expression<Func<TEntity, T>> column)
        {
            return await this.context.MinAsync<TEntity, T>(predicate, column, this.GetType(), true).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> GroupByAsync<T>(Expression<Func<TEntity, bool>> predicate, Expression<Func<TEntity, T>> column)
        {
            return await this.context.GroupByAsync<TEntity, T>(predicate, column, this.GetType(), true).ConfigureAwait(false);
        }
    }
}
