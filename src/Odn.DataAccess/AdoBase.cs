using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Data.SqlClient;

namespace Odn.DataAccess
{
    public abstract class AdoBase
    {
        protected abstract string ConnectionName { get; }

        #region TransScope

        /// <summary>
        /// 用一个事物执行多个数据库操作
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="action"></param>
        /// <param name="trans">Trans=NULL会创建一个新的Connection, 并开启新的事物, 不过再此处可以选择传入SessionTrans来在NHibernate Session的基础上执行，避免开启新的Connection, 以节省Connection资源</param>
        /// <param name="isolationLevel"></param>
        /// <returns></returns>
        protected T TransScope<T>(Func<IDbTransaction, T> action, IDbTransaction trans = null, IsolationLevel isolationLevel = IsolationLevel.Unspecified)
        {
            if (trans != null)
            {
                return action(trans);
            }

            IDbConnection conn = null;
            bool isTrans = false;
            GetConnection(trans, out conn, out isTrans);

            try
            {
                DbConnectionManager.Instance.SafeOpenConnection(conn);
                trans = conn.BeginTransaction(isolationLevel);
                var result = action(trans);
                trans.Commit();
                return result;
            }
            catch (Exception)
            {
                //rollback when err
                if (trans != null)
                    trans.Rollback();
                throw;
            }
            finally
            {
                if (trans != null)
                    trans.Dispose();
                //close conn
                DbConnectionManager.Instance.SafeCloseConnection(conn);
            }
        }

        protected void TransScope(Action<IDbTransaction> action, IDbTransaction trans = null, IsolationLevel isolationLevel = IsolationLevel.Unspecified)
        {
            TransScope(t => { action(t); return string.Empty; }, trans, isolationLevel);
        }


        #endregion

        #region Warpped Dapper Internal Method and Logging
        /// <summary>
        /// 通过当前的 outerTrans执行当前的Action, 
        /// Action本身需要一个Connection与一个Transaction
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="action"></param>
        /// <param name="outerTrans"></param>
        /// <returns></returns>
        protected int Execute(string sql, object param = null, IDbTransaction outerTrans = null)
        {
            WriteLog(sql, param);

            bool isTrans;
            IDbConnection conn = null;
            GetConnection(outerTrans, out conn, out isTrans);

            try
            {
                DbConnectionManager.Instance.SafeOpenConnection(conn);
                return conn.Execute(sql, param, outerTrans);
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                //cmd.Dispose();
                if (!isTrans)
                    DbConnectionManager.Instance.SafeCloseConnection(conn);
            }
        }

        protected T Scalar<T>(string sql, object param = null, IDbTransaction outerTrans = null)
        {
            WriteLog(sql, param);

            bool isTrans;
            IDbConnection conn = null;
            GetConnection(outerTrans, out conn, out isTrans);

            try
            {
                DbConnectionManager.Instance.SafeOpenConnection(conn);
                return conn.ExecuteScalar<T>(sql, param, outerTrans);
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                //cmd.Dispose();
                if (!isTrans)
                    DbConnectionManager.Instance.SafeCloseConnection(conn);
            }
        }

        protected T Get<T>(string sql, object param = null, IDbTransaction outerTrans = null)
        {
            WriteLog(sql, param);

            bool isTrans;
            IDbConnection conn = null;
            GetConnection(outerTrans, out conn, out isTrans);

            try
            {
                DbConnectionManager.Instance.SafeOpenConnection(conn);
                return conn.Query<T>(sql, param, outerTrans).FirstOrDefault();
            }
            catch (Exception)
            {
                //WriteLog("sql err msg", ex.ToString());
                //if (cmd != null)
                //    WriteLog("sql err", SqlCommandHelper.SqlCommandToString(cmd));
                throw;
            }
            finally
            {
                //cmd.Dispose();
                if (!isTrans)
                    DbConnectionManager.Instance.SafeCloseConnection(conn);
            }
        }
        protected IEnumerable<T> GetList<T>(string sql, object param = null, IDbTransaction outerTrans = null)
        {
            WriteLog(sql, param);

            bool isTrans;
            IDbConnection conn = null;
            GetConnection(outerTrans, out conn, out isTrans);

            try
            {
                DbConnectionManager.Instance.SafeOpenConnection(conn);
                return conn.Query<T>(sql, param, outerTrans);
            }
            catch (Exception)
            {
                //WriteLog("sql err msg", ex.ToString());
                //if (cmd != null)
                //    WriteLog("sql err", SqlCommandHelper.SqlCommandToString(cmd));
                throw;
            }
            finally
            {
                //cmd.Dispose();
                if (!isTrans)
                    DbConnectionManager.Instance.SafeCloseConnection(conn);
            }
        }

        protected DataSet GetSqlDataSet(string sql, IDbTransaction outerTrans = null)
        {
            bool isTrans;
            IDbConnection conn = null;
            GetConnection(outerTrans, out conn, out isTrans);

            try
            {
                DbConnectionManager.Instance.SafeOpenConnection(conn);
                DataSet ds = new DataSet();

                SqlDataAdapter sda = new SqlDataAdapter(sql, conn as SqlConnection);
                sda.Fill(ds);
                return ds;
            }
            catch (Exception)
            {
                //WriteLog("sql err msg", ex.ToString());
                //if (cmd != null)
                //    WriteLog("sql err", SqlCommandHelper.SqlCommandToString(cmd));
                throw;
            }
            finally
            {
                //cmd.Dispose();
                if (!isTrans)
                    DbConnectionManager.Instance.SafeCloseConnection(conn);
            }

        }

        private void GetConnection(IDbTransaction trans, out IDbConnection conn, out bool isTransaction)
        {
            if (trans == null)
            {
                conn = DbConnectionManager.Instance.GetConnection(ConnectionName);
                isTransaction = false;
            }
            else
            {
                conn = trans.Connection;
                isTransaction = true;
            }
        }

        protected virtual void WriteLog(string sql, object parms)
        {
            //if (HttpContext.Current == null) return;
            //HttpContext.Current.Trace.Write("Dapper.NET", SqlCommandToStr.CommandAsSql(sql, parms));
            //HttpContext.Current.Trace.Write("Dapper.NET Parms", sql);
        }
        #endregion
    }
}
