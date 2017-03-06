﻿namespace FSharp.Data.Sql.Runtime

open System
open System.Collections
open System.Collections.Generic
open System.Data

open FSharp.Data.Sql
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Common.Utilities
open FSharp.Data.Sql.QueryExpression
open FSharp.Data.Sql.Schema

// this is publically exposed and used in the runtime
type IWithDataContext =
    abstract DataContext : ISqlDataContext

module internal QueryImplementation =
    open System.Linq
    open System.Linq.Expressions
    open Patterns

    type IWithSqlService =
        abstract DataContext : ISqlDataContext
        abstract SqlExpression : SqlExp
        abstract TupleIndex : string ResizeArray // indexes where in the anonymous object created by the compiler during a select many that each entity alias appears
        abstract Provider : ISqlProvider
    
    /// Interface for async enumerations as .NET doesn't have it out-of-the-box
    type IAsyncEnumerable<'T> =
        abstract GetAsyncEnumerator : unit -> Async<IEnumerator<'T>>

    let (|SourceWithQueryData|_|) = function Constant ((:? IWithSqlService as org), _)    -> Some org | _ -> None
    let (|RelDirection|_|)        = function Constant ((:? RelationshipDirection as s),_) -> Some s   | _ -> None

    let closeConnection (provider : ISqlProvider) (con : IDbConnection) = 
        #if NO_MS_ACCESS
            con.Close()
        #else  
            match provider with
            | :? Providers.MSAccessProvider -> ()  //else get 'COM object that has been separated from its underlying RCW cannot be used.'
            | _ -> con.Close()
        #endif

    let parseQueryResults (projector:Delegate) (results:SqlEntity[]) =
        let args = projector.GetType().GenericTypeArguments
        seq { 
            if args.Length > 0 && args.[0].Name.StartsWith("IGrouping") then
                // do group-read
                let collected = 
                    results |> Array.map(fun (e:SqlEntity) ->
                        let aggregates = [|"[COUNT]"; "[MIN]"; "[MAX]"; "[SUM]"; "[AVG]"|]
                        let data = e.ColumnValues |> Seq.toArray |> Array.filter(fun (key, _) -> aggregates |> Array.exists (key.Contains) |> not)
                        let results =
                            data |> Array.map(fun (keyname, keyvalueb) ->
                                let keyvalue = unbox(keyvalueb)
                                let ty = typedefof<GroupResultItems<_>>.MakeGenericType(keyvalue.GetType())
                                let grp = ty.GetConstructors().[0].Invoke [|keyname; keyvalue; e;|]
                                grp)
                        // database will give distinct SqlEntities to have groups.
                        // If multi-key-columns, the aggregation values of 
                        // GroupResultItems distinctItem.ColumnValues should be handled.
                        match results with
                        | [||] -> failwith "aggregate not found"
                        | [|x|] -> x
                        | lst -> failwith "multiple key columns not supported yet"
                    )// :?> IGrouping<_, _>)

                for e in collected -> projector.DynamicInvoke(e) 
            else
                for e in results -> projector.DynamicInvoke(e) 
        } |> Seq.cache :> System.Collections.IEnumerable

    let executeQuery (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
        use con = provider.CreateConnection(dc.ConnectionString)
        let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression sqlExp ti con provider
        let paramsString = parameters |> Seq.fold (fun acc p -> acc + (sprintf "%s - %A; " p.ParameterName p.Value)) ""
        Common.QueryEvents.PublishSqlQuery (sprintf "%s - params %s" query paramsString)
        // todo: make this lazily evaluated? or optionally so. but have to deal with disposing stuff somehow
        use cmd = provider.CreateCommand(con,query)
        for p in parameters do cmd.Parameters.Add p |> ignore
        let columns = provider.GetColumns(con, baseTable)
        if con.State <> ConnectionState.Open then con.Open()
        use reader = cmd.ExecuteReader()
        let results = dc.ReadEntities(baseTable.FullName, columns, reader)
        let results = parseQueryResults projector results
        closeConnection provider con
        results

    let executeQueryAsync (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       async {
           use con = provider.CreateConnection(dc.ConnectionString) :?> System.Data.Common.DbConnection
           let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression sqlExp ti con provider
           let paramsString = parameters |> Seq.fold (fun acc p -> acc + (sprintf "%s - %A; " p.ParameterName p.Value)) ""
           Common.QueryEvents.PublishSqlQuery (sprintf "%s - params %s" query paramsString)
           // todo: make this lazily evaluated? or optionally so. but have to deal with disposing stuff somehow
           use cmd = provider.CreateCommand(con,query) :?> System.Data.Common.DbCommand
           for p in parameters do cmd.Parameters.Add p |> ignore
           let columns = provider.GetColumns(con, baseTable) // TODO : provider.GetColumnsAsync() ??
           if con.State <> ConnectionState.Open then
                do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           use! reader = cmd.ExecuteReaderAsync() |> Async.AwaitTask
           let! results = dc.ReadEntitiesAsync(baseTable.FullName, columns, reader)
           let results = parseQueryResults projector results
           closeConnection provider con
           return results
       }

    let executeQueryScalar (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       use con = provider.CreateConnection(dc.ConnectionString)
       con.Open()
       let (query,parameters,_,_) = QueryExpressionTransformer.convertExpression sqlExp ti con provider
       Common.QueryEvents.PublishSqlQuery (sprintf "%s - params %A" query parameters)
       use cmd = provider.CreateCommand(con,query)
       for p in parameters do cmd.Parameters.Add p |> ignore
       // ignore any generated projection and just expect a single integer back
       if con.State <> ConnectionState.Open then con.Open()
       let result = cmd.ExecuteScalar()
       closeConnection provider con
       result

    let executeQueryScalarAsync (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       async {
           use con = provider.CreateConnection(dc.ConnectionString) :?> System.Data.Common.DbConnection
           do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           let (query,parameters,_,_) = QueryExpressionTransformer.convertExpression sqlExp ti con provider
           Common.QueryEvents.PublishSqlQuery (sprintf "%s - params %A" query parameters)
           use cmd = provider.CreateCommand(con,query) :?> System.Data.Common.DbCommand
           for p in parameters do cmd.Parameters.Add p |> ignore
           // ignore any generated projection and just expect a single integer back
           if con.State <> ConnectionState.Open then
                do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           let! executed = cmd.ExecuteScalarAsync() |> Async.AwaitTask
           closeConnection provider con
           return executed
       }

    type SqlQueryable<'T>(dc:ISqlDataContext,provider,sqlQuery,tupleIndex) =
        static member Create(table,conString,provider) =
            SqlQueryable<'T>(conString,provider,BaseTable("",table),ResizeArray<_>()) :> IQueryable<'T>
        interface IQueryable<'T>
        interface IQueryable with
            member __.Provider = SqlQueryProvider.Provider
            member x.Expression =  Expression.Constant(x,typeof<IQueryable<'T>>) :> Expression
            member __.ElementType = typeof<'T>
        interface seq<'T> with
             member __.GetEnumerator() = (Seq.cast<'T> (executeQuery dc provider sqlQuery tupleIndex)).GetEnumerator()
        interface IEnumerable with
             member x.GetEnumerator() = (x :> seq<'T>).GetEnumerator() :> IEnumerator
        interface IWithDataContext with
             member __.DataContext = dc
        interface IWithSqlService with
             member __.DataContext = dc
             member __.SqlExpression = sqlQuery
             member __.TupleIndex = tupleIndex
             member __.Provider = provider
        interface IAsyncEnumerable<'T> with
             member __.GetAsyncEnumerator() =
                async {
                    let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                    return (Seq.cast<'T> (executeSql)).GetEnumerator()
                }
    
    and SqlOrderedQueryable<'T>(dc:ISqlDataContext,provider,sqlQuery,tupleIndex) =
        static member Create(table,conString,provider) =
            SqlOrderedQueryable<'T>(conString,provider,BaseTable("",table),ResizeArray<_>()) :> IQueryable<'T>
        interface IOrderedQueryable<'T>
        interface IQueryable<'T>
        interface IQueryable with
            member __.Provider = SqlQueryProvider.Provider
            member x.Expression =  Expression.Constant(x,typeof<IOrderedQueryable<'T>>) :> Expression
            member __.ElementType = typeof<'T>
        interface seq<'T> with
             member __.GetEnumerator() = (Seq.cast<'T> (executeQuery dc provider sqlQuery tupleIndex)).GetEnumerator()
        interface IEnumerable with
             member x.GetEnumerator() = (x :> seq<'T>).GetEnumerator() :> IEnumerator
        interface IWithDataContext with
            member __.DataContext = dc
        interface IWithSqlService with
             member __.DataContext = dc
             member __.SqlExpression = sqlQuery
             member __.TupleIndex = tupleIndex
             member __.Provider = provider
        interface IAsyncEnumerable<'T> with
             member __.GetAsyncEnumerator() =
                async {
                    let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                    return (Seq.cast<'T> (executeSql)).GetEnumerator()
                }

    /// Structure to make it easier to return IGrouping from GroupBy
    and SqlGroupingQueryable<'TKey, 'TEntity>(dc:ISqlDataContext,provider,sqlQuery,tupleIndex) =
        static member Create(table,conString,provider) =
            let res = SqlGroupingQueryable<'TKey, 'TEntity>(conString,provider,BaseTable("",table),ResizeArray<_>())
            res :> IQueryable<IGrouping<'TKey, 'TEntity>>
        interface IQueryable<IGrouping<'TKey, 'TEntity>>
        interface IQueryable with
            member __.Provider = 
                SqlQueryProvider.Provider
            member x.Expression =  
                Expression.Constant(x,typeof<SqlGroupingQueryable<'TKey, 'TEntity>>) :> Expression
            member __.ElementType = 
                typeof<IGrouping<'TKey, 'TEntity>>
        interface seq<IGrouping<'TKey, 'TEntity>> with
             member __.GetEnumerator() = 
                executeQuery dc provider sqlQuery tupleIndex
                |> Seq.cast<IGrouping<'TKey, 'TEntity>>
                |> fun res -> res.GetEnumerator()
        interface IEnumerable with
             member x.GetEnumerator() = 
                let itms = (x :> seq<IGrouping<'TKey, 'TEntity>>)
                itms.GetEnumerator() :> IEnumerator
        interface IWithDataContext with
             member __.DataContext = dc
        interface IWithSqlService with
             member __.DataContext = dc
             member __.SqlExpression = sqlQuery
             member __.TupleIndex = tupleIndex
             member __.Provider = provider
        interface IAsyncEnumerable<IGrouping<'TKey, 'TEntity>> with
             member __.GetAsyncEnumerator() =
                async {
                    let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                    let toseq = executeSql |> Seq.cast<IGrouping<'TKey, 'TEntity>>
                    return toseq.GetEnumerator()
                }
    and SqlWhereType = NormalWhere | HavingWhere
    and SqlQueryProvider() =
         static member val Provider =

             let parseWhere (meth:Reflection.MethodInfo) (source:IWithSqlService) (qual:Expression) =
                let paramNames = HashSet<string>()

                let isHaving =
                    let rec checkExpression = function
                        | SelectMany(_, _,GroupQuery(_),_) -> true
                        | HavingClause(f,ex) -> true
                        | _ -> false
                    checkExpression source.SqlExpression

                let (|Condition|_|) exp =
                    // IMPORTANT : for now it is always assumed that the table column being checked on the server side is on the left hand side of the condition expression.
                    match exp with
                    | SqlSpecialOpArrQueryable(ti,op,key,qry)
                    | SqlSpecialNegativeOpArrQueryable(ti,op,key,qry) ->

                        let svc = (qry :?> IWithSqlService)
                        use con = svc.Provider.CreateConnection(svc.DataContext.ConnectionString)
                        let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression svc.SqlExpression svc.TupleIndex con svc.Provider

                        let modified = 
                            parameters |> Seq.map(fun p ->
                                p.ParameterName <- p.ParameterName.Replace("@param", "@paramnested")
                                p
                            ) |> Seq.toArray
                        let subquery = 
                            let paramfixed = query.Replace("@param", "@paramnested")
                            match paramfixed.EndsWith(";") with
                            | false -> paramfixed
                            | true -> paramfixed.Substring(0, paramfixed.Length-1)
                        
                        Some(ti,key,op,Some (box (subquery, modified)))
                    | SqlSpecialOpArr(ti,op,key,value)
                    | SqlSpecialNegativeOpArr(ti,op,key,value) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,op,Some (box value))
                    | SqlSpecialOp(ti,op,key,value) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,op,Some value)
                    // if using nullable types
                    | OptionIsSome(SqlColumnGet(ti,key,_)) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.NotNull,None)
                    | OptionIsNone(SqlColumnGet(ti,key,_))
                    | SqlCondOp(ConditionOperator.Equal,(SqlColumnGet(ti,key,_)),OptionNone) 
                    | SqlNegativeCondOp(ConditionOperator.Equal,(SqlColumnGet(ti,key,_)),OptionNone) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.IsNull,None)
                    | SqlCondOp(ConditionOperator.NotEqual,(SqlColumnGet(ti,key,_)),OptionNone) 
                    | SqlNegativeCondOp(ConditionOperator.NotEqual,(SqlColumnGet(ti,key,_)),OptionNone) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.NotNull,None)
                    // matches column to constant with any operator eg c.name = "john", c.age > 42
                    | SqlCondOp(op,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant(c)))) 
                    | SqlNegativeCondOp(op,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant(c)))) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,op,c)
                    // matches to another property getter, method call or new expression
                    | SqlCondOp(op,OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_)),OptionalConvertOrTypeAs(OptionalFSharpOptionValue((((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth))))
                    | SqlNegativeCondOp(op,OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_)),OptionalConvertOrTypeAs(OptionalFSharpOptionValue((((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth)))) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,op,Some(Expression.Lambda(meth).Compile().DynamicInvoke()))
                    | SqlColumnGet(ti,key,ret) when exp.Type.FullName = "System.Boolean" -> 
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.Equal, Some(true |> box))
                    | _ -> None

                let (|HavingCondition|_|) exp =
                    // IMPORTANT : for now it is always assumed that the table column being checked on the server side is on the left hand side of the condition expression.
                    match exp with
                    | OptionIsSome(SqlGroupingColumnGet(ti,key,_)) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.NotNull,None)
                    | OptionIsNone(SqlGroupingColumnGet(ti,key,_))
                    | SqlCondOp(ConditionOperator.Equal,(SqlGroupingColumnGet(ti,key,_)),OptionNone) 
                    | SqlNegativeCondOp(ConditionOperator.Equal,(SqlGroupingColumnGet(ti,key,_)),OptionNone) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.IsNull,None)
                    | SqlCondOp(ConditionOperator.NotEqual,(SqlGroupingColumnGet(ti,key,_)),OptionNone) 
                    | SqlNegativeCondOp(ConditionOperator.NotEqual,(SqlGroupingColumnGet(ti,key,_)),OptionNone) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.NotNull,None)
                    // matches column to constant with any operator eg c.name = "john", c.age > 42
                    | SqlCondOp(op,(OptionalConvertOrTypeAs(SqlGroupingColumnGet(ti,key,_))),OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant(c)))) 
                    | SqlNegativeCondOp(op,(OptionalConvertOrTypeAs(SqlGroupingColumnGet(ti,key,_))),OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant(c)))) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,op,c)
                    // matches to another property getter, method call or new expression
                    | SqlCondOp(op,OptionalConvertOrTypeAs(SqlGroupingColumnGet(ti,key,_)),OptionalConvertOrTypeAs(OptionalFSharpOptionValue((((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth))))
                    | SqlNegativeCondOp(op,OptionalConvertOrTypeAs(SqlGroupingColumnGet(ti,key,_)),OptionalConvertOrTypeAs(OptionalFSharpOptionValue((((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth)))) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,op,Some(Expression.Lambda(meth).Compile().DynamicInvoke()))
                    | SqlGroupingColumnGet(ti,key,ret) when exp.Type.FullName = "System.Boolean" -> 
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.Equal, Some(true |> box))
                    | _ -> None

                let rec filterExpression (exp:Expression)  =
                    let extendFilter conditions nextFilter =
                        match exp with
                        | AndAlso(_) -> And(conditions,nextFilter)
                        | OrElse(_) -> Or(conditions,nextFilter)
                        | _ -> failwith ("Filter problem: " + exp.ToString())
                    match exp with
                    | AndAlsoOrElse(AndAlsoOrElse(_,_) as left, (AndAlsoOrElse(_,_) as right)) ->
                        extendFilter [] (Some ([filterExpression left; filterExpression right]))
                    | AndAlsoOrElse(AndAlsoOrElse(_,_) as left,Condition(c))  ->
                        extendFilter [c] (Some ([filterExpression left]))
                    | AndAlsoOrElse(Condition(c),(AndAlsoOrElse(_,_) as right))  ->
                        extendFilter [c] (Some ([filterExpression right]))
                    | AndAlsoOrElse(Condition(c1) as cc1 ,Condition(c2)) as cc2 ->
                        if cc1 = cc2 then extendFilter [c1] None
                        else extendFilter [c1;c2] None
                    | Condition(cond) ->
                        Condition.And([cond],None)

                    // Support for simple boolean expressions:
                    | AndAlso(Bool(b), x) | AndAlso(x, Bool(b)) when b = true -> filterExpression x
                    | OrElse(Bool(b), x) | OrElse(x, Bool(b)) when b = false -> filterExpression x
                    | Bool(b) when b -> Condition.ConstantTrue
                    | Bool(b) when not(b) -> Condition.ConstantFalse
                    | _ -> 
                        if isHaving then
                            match exp with
                            | HavingCondition(cond) -> Condition.And([cond],None)
                            | _ -> failwith ("Unsupported group having expression. " + exp.ToString())
                        else
                        failwith ("Unsupported expression. Ensure all server-side objects appear on the left hand side of predicates.  The In and Not In operators only support the inline array syntax. " + exp.ToString())

                match qual with
                | Lambda([name],ex) ->
                    // name here will either be the alias the user entered in the where clause if no joining / select many has happened before this
                    // otherwise, it will be the compiler-generated alias eg _arg2.  this might be the first method called in which case set the
                    // base entity alias to this name.
                    let ex = ExpressionOptimizer.visit ex
                    let filter = filterExpression ex
                    let sqlExpression =
                        match source.SqlExpression with
                        | BaseTable(alias,entity) when alias = "" ->
                            // special case here as above - this is the first call so replace the top of the tree here with the current base entity alias and the filter
                            FilterClause(filter,BaseTable(name.Name,entity))
                        | current ->
                            if isHaving then HavingClause(filter,current)
                            else

                            // the following case can happen with multiple where clauses when only a single entity is selected
                            if paramNames.First() = "" || source.TupleIndex.Count = 0 then FilterClause(filter,current)
                            else FilterClause(filter,current)

                    let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                    ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                | _ -> failwith "only support lambdas in a where"

             let parseGroupBy (meth:Reflection.MethodInfo) (source:IWithSqlService) sourceAlias destAlias (lambdas: LambdaExpression list) (exp:Expression) (sourceTi:string)=
                let sourceEntity =
                    match source.SqlExpression with
                    | BaseTable(alias,sourceEntity) ->
                        if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                        if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                        sourceEntity

                    | SelectMany(a1, a2,selectdata,sqlExp)  ->
                        let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                        if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                        if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                        failwithf "Grouping over multiple tables is not supported yet"
                    | _ -> failwithf "Unexpected groupby entity expression (%A)." source.SqlExpression

                let getAlias ti =
                        match ti with
                        | "" when source.SqlExpression.HasAutoTupled() -> sourceAlias
                        | "" -> ""
                        | _ -> resolveTuplePropertyName ti source.TupleIndex

                let keycols = 
                    match exp with
                    | SqlColumnGet(sourceTi,sourceKey,_) -> [getAlias sourceTi, sourceKey]
                    | TupleSqlColumnsGet itms -> itms |> List.map(fun (ti,key,typ) -> getAlias ti, key)
                    | _ -> []

                let data =  {
                    PrimaryTable = sourceEntity
                    KeyColumns = keycols
                    AggregateColumns = [] // Aggregates will be populated later: [CountOp,alias,"City"]
                    Projection = None //lambda2 ?
                }
                let ty = typedefof<SqlGroupingQueryable<_,_>>.MakeGenericType(lambdas.[0].ReturnType, meth.GetGenericArguments().[0])
                ty, data, sourceEntity.Name

             // Possible Linq method overrides are available here: 
             // https://referencesource.microsoft.com/#System.Core/System/Linq/IQueryable.cs
             // https://msdn.microsoft.com/en-us/library/system.linq.enumerable_methods(v=vs.110).aspx
             { new System.Linq.IQueryProvider with
                member __.CreateQuery(e:Expression) : IQueryable = failwithf "CreateQuery, e = %A" e
                member __.CreateQuery<'T>(e:Expression) : IQueryable<'T> =
                    Common.QueryEvents.PublishExpression e
                    match e with
                    | MethodCall(None, (MethodWithName "Skip" as meth), [SourceWithQueryData source; Int amount]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        ty.GetConstructors().[0].Invoke [| source.DataContext ; source.Provider; Skip(amount,source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "Take" as meth), [SourceWithQueryData source; Int amount]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        ty.GetConstructors().[0].Invoke [| source.DataContext ; source.Provider; Take(amount,source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "OrderBy" | MethodWithName "OrderByDescending" as meth), [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]) ->
                        let alias =
                             match entity with
                             | "" when source.SqlExpression.HasAutoTupled() -> param
                             | "" -> ""
                             | _ -> Utilities.resolveTuplePropertyName entity source.TupleIndex
                        let ascending = meth.Name = "OrderBy"
                        let sqlExpression =
                               match source.SqlExpression with
                               | BaseTable("",entity)  -> OrderBy("",key,ascending,BaseTable(alias,entity))
                               | _ ->  OrderBy(alias,key,ascending,source.SqlExpression)
                        let ty = typedefof<SqlOrderedQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        let x = ty.GetConstructors().[0].Invoke [| source.DataContext ; source.Provider; sqlExpression; source.TupleIndex; |]
                        x :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "ThenBy" | MethodWithName "ThenByDescending" as meth), [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]) ->
                        let alias =
                            match entity with
                            | "" when source.SqlExpression.HasAutoTupled() -> param
                            | "" -> ""
                            | _ -> Utilities.resolveTuplePropertyName entity source.TupleIndex
                        let ty = typedefof<SqlOrderedQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        let ascending = meth.Name = "ThenBy"
                        match source.SqlExpression with
                        | OrderBy(_) ->
                            let x = ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; OrderBy(alias,key,ascending,source.SqlExpression) ; source.TupleIndex; |]
                            x :?> IQueryable<_>
                        | _ -> failwith (sprintf "'thenBy' operations must come immediately after a 'sortBy' operation in a query")

                    | MethodCall(None, (MethodWithName "Distinct" as meth), [ SourceWithQueryData source ]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; Distinct(source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "Where" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        parseWhere meth source qual
//                    | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
//                                    [ SourceWithQueryData source;
//                                      OptionalQuote (Lambda([ParamName lambdaparam], exp) as lambda1);
//                                      OptionalQuote (Lambda([ParamName _], _))
//                                      OptionalQuote (Lambda([ParamName _], _))]) 
//                    | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
//                                    [ SourceWithQueryData source;
//                                      OptionalQuote (Lambda([ParamName lambdaparam], exp) as lambda1);
//                                      OptionalQuote (Lambda([ParamName _], _))]) 
                    | MethodCall(None, (MethodWithName "GroupBy" as meth),
                                    [ SourceWithQueryData source;
                                      OptionalQuote (Lambda([ParamName lambdaparam], exp) as lambda1)]) ->
                        let lambda = lambda1 :?> LambdaExpression
                        let ty, data, sourceEntityName = parseGroupBy meth source lambdaparam "" [lambda] exp ""
                        let expr = SelectMany(sourceEntityName,"grp",GroupQuery(data), source.SqlExpression)

                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; expr; source.TupleIndex;|] :?> IQueryable<'T>

                    | MethodCall(None, (MethodWithName "Join"),
                                    [ SourceWithQueryData source;
                                      SourceWithQueryData dest
                                      OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))
                                      OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(_,destKey,_)))
                                      OptionalQuote projection ]) ->
                        let destEntity =
                            match dest.SqlExpression with
                            | BaseTable(_,destEntity) -> destEntity
                            | _ -> failwithf "Unexpected join destination entity expression (%A)." dest.SqlExpression
                        let sqlExpression =
                            match source.SqlExpression with
                            | BaseTable(alias,entity) when alias = "" ->
                                // special case here as above - this is the first call so replace the top of the tree here with the current base table alias and the select many
                                let data = { PrimaryKey = [destKey]; PrimaryTable = destEntity; ForeignKey = [sourceKey]; ForeignTable = entity; OuterJoin = false; RelDirection = RelationshipDirection.Parents}
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                SelectMany(sourceAlias,destAlias, LinkQuery(data),BaseTable(sourceAlias,entity))
                            | _ ->
                                let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                // we don't actually have the "foreign" table name here in a join as that information is "lost" further up the expression tree.
                                // it's ok though because it can always be resolved later after the whole expression tree has been evaluated
                                let data = { PrimaryKey = [destKey]; PrimaryTable = destEntity; ForeignKey = [sourceKey];
                                             ForeignTable = {Schema="";Name="";Type=""};
                                             OuterJoin = false; RelDirection = RelationshipDirection.Parents }
                                SelectMany(sourceAlias,destAlias,LinkQuery(data),source.SqlExpression)

                        let ty =
                            match projection with
                                | :? LambdaExpression as meth -> typedefof<SqlQueryable<_>>.MakeGenericType(meth.ReturnType)
                                | _ -> failwith "unsupported projection in join"
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                    | MethodCall(None, (MethodWithName "Join"),
                                    [ SourceWithQueryData source;
                                      SourceWithQueryData dest
                                      OptionalQuote (Lambda([ParamName sourceAlias],TupleSqlColumnsGet(multisource)))
                                      OptionalQuote (Lambda([ParamName destAlias],TupleSqlColumnsGet(multidest)))
                                      OptionalQuote projection ]) ->
                        let destEntity =
                            match dest.SqlExpression with
                            | BaseTable(_,destEntity) -> destEntity
                            | _ -> failwithf "Unexpected join destination entity expression (%A)." dest.SqlExpression
                        let destKeys = multidest |> List.map(fun(_,dest,_)->dest)
                        let sourceKeys = multisource |> List.map(fun(_,source,_)->source)
                        let sqlExpression =
                            match source.SqlExpression with
                            | BaseTable(alias,entity) when alias = "" ->
                                // special case here as above - this is the first call so replace the top of the tree here with the current base table alias and the select many
                                let data = { PrimaryKey = destKeys; PrimaryTable = destEntity; ForeignKey = sourceKeys; ForeignTable = entity; OuterJoin = false; RelDirection = RelationshipDirection.Parents}
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                SelectMany(sourceAlias,destAlias, LinkQuery(data),BaseTable(sourceAlias,entity))
                            | _ ->
                                let sourceTi = multisource |> List.tryPick(fun(ti,_,_)->match ti with "" -> None | x -> Some x)
                                let sourceAlias = match sourceTi with None -> sourceAlias | Some x -> Utilities.resolveTuplePropertyName x source.TupleIndex
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                // we don't actually have the "foreign" table name here in a join as that information is "lost" further up the expression tree.
                                // it's ok though because it can always be resolved later after the whole expression tree has been evaluated
                                let data = { PrimaryKey = destKeys; PrimaryTable = destEntity; ForeignKey = sourceKeys;
                                             ForeignTable = {Schema="";Name="";Type=""};
                                             OuterJoin = false; RelDirection = RelationshipDirection.Parents }
                                SelectMany(sourceAlias,destAlias,LinkQuery(data),source.SqlExpression)

                        let ty =
                            match projection with
                                | :? LambdaExpression as meth -> typedefof<SqlQueryable<_>>.MakeGenericType(meth.ReturnType)
                                | _ -> failwith "unsupported projection in join"
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "SelectMany"),
                                    [ SourceWithQueryData source;
                                      OptionalQuote (Lambda([_], inner ));
                                      OptionalQuote (Lambda(projectionParams,_) as projection)  ]) ->
                        let ty =
                            match projection with
                                | :? LambdaExpression as meth -> typedefof<SqlQueryable<_>>.MakeGenericType(meth.ReturnType)
                                | _ -> failwith "unsupported projection in select many"

                        // multiple SelectMany calls in sequence are represented in the same expression tree which must be parsed recursively (and joins too!)
                        let rec processSelectManys toAlias inExp outExp =
                            let (|OptionalOuterJoin|) e =
                                match e with
                                | MethodCall(None, (!!), [inner]) -> (true,inner)
                                | _ -> (false,e)
                            match inExp with
                            | MethodCall(None, (MethodWithName "SelectMany"), [ createRelated ; OptionalQuote (Lambda([_], inner)); OptionalQuote (Lambda(projectionParams,_)) ]) ->
                                let outExp = processSelectManys projectionParams.[0].Name createRelated outExp
                                processSelectManys projectionParams.[1].Name inner outExp
//                            | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
//                                                    [createRelated
//                                                     ConvertOrTypeAs(MethodCall(Some(Lambda(_,MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_))
//                                                     OptionalQuote (Lambda([ParamName sourceAlias], exp) as lambda1)
//                                                     OptionalQuote (Lambda(_,_))])
//                            | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
//                                                    [createRelated
//                                                     ConvertOrTypeAs(MethodCall(_, (MethodWithName "CreateEntities"), [String destEntity] ))
//                                                     OptionalQuote (Lambda([ParamName sourceAlias], exp) as lambda1)
//                                                     OptionalQuote (Lambda(_,_))]) 
//                            | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
//                                                    [createRelated
//                                                     ConvertOrTypeAs(MethodCall(Some(Lambda(_,MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_))
//                                                     OptionalQuote (Lambda([ParamName sourceAlias], exp) as lambda1)])
                            | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
                                                    [createRelated
                                                     ConvertOrTypeAs(MethodCall(_, (MethodWithName "CreateEntities"), [String destEntity] ))
                                                     OptionalQuote (Lambda([ParamName sourceAlias], exp) as lambda1)]) ->

                                let lambda = lambda1 :?> LambdaExpression
                                let outExp = processSelectManys projectionParams.[0].Name createRelated outExp
                                let ty, data, sourceEntityName = parseGroupBy meth source sourceAlias destEntity [lambda] exp ""
                                SelectMany(sourceAlias,destEntity,GroupQuery(data), outExp)

                            | MethodCall(None, (MethodWithName "Join"),
                                                    [createRelated
                                                     ConvertOrTypeAs(MethodCall(Some(Lambda(_,MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_))
                                                     OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))
                                                     OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(_,destKey,_)))
                                                     OptionalQuote (Lambda(projectionParams,_))])
                            | MethodCall(None, (MethodWithName "Join"),
                                                    [createRelated
                                                     ConvertOrTypeAs(MethodCall(_, (MethodWithName "CreateEntities"), [String destEntity] ))
                                                     OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))
                                                     OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(_,destKey,_)))
                                                     OptionalQuote (Lambda(projectionParams,_))]) ->
                                // this case happens when the select many also includes one or more joins in the same tree.
                                // in this situation, the first agrument will either be an additional nested join method call,
                                // or finally it will be the call to _CreatedRelated which is handled recursively in the next case
                                let outExp = processSelectManys projectionParams.[0].Name createRelated outExp
                                let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                // we don't actually have the "foreign" table name here in a join as that information is "lost" further up the expression tree.
                                // it's ok though because it can always be resolved later after the whole expression tree has been evaluated
                                let data = { PrimaryKey = [destKey]; PrimaryTable = Table.FromFullName destEntity; ForeignKey = [sourceKey];
                                                ForeignTable = {Schema="";Name="";Type=""};
                                                OuterJoin = false; RelDirection = RelationshipDirection.Parents }
                                SelectMany(sourceAlias,destAlias,LinkQuery(data),outExp)
                            | OptionalOuterJoin(outerJoin,MethodCall(Some(_),(MethodWithName "CreateRelated"), [param; _; String PE; String PK; String FE; String FK; RelDirection dir;])) ->
                                let fromAlias =
                                    match param with
                                    | ParamName x -> x
                                    | PropertyGet(_,p) -> Utilities.resolveTuplePropertyName p.Name source.TupleIndex
                                    | _ -> failwith "unsupported parameter expression in CreatedRelated method call"
                                let data = { PrimaryKey = [PK]; PrimaryTable = Table.FromFullName PE; ForeignKey = [FK]; ForeignTable = Table.FromFullName FE; OuterJoin = outerJoin; RelDirection = dir  }
                                let sqlExpression =
                                    match outExp with
                                    | BaseTable(alias,entity) when alias = "" ->
                                        // special case here as above - this is the first call so replace the top of the tree here with the current base entity alias and the select many
                                        SelectMany(fromAlias,toAlias,LinkQuery(data),BaseTable(alias,entity))
                                    | _ ->
                                        SelectMany(fromAlias,toAlias,LinkQuery(data),outExp)
                                // add new aliases to the tuple index
                                if source.TupleIndex.Any(fun v -> v = fromAlias) |> not then source.TupleIndex.Add(fromAlias)
                                if source.TupleIndex.Any(fun v -> v = toAlias) |> not then  source.TupleIndex.Add(toAlias)
                                sqlExpression
                            | MethodCall(None, (MethodWithName "Join"),
                                                    [createRelated
                                                     ConvertOrTypeAs(MethodCall(Some(Lambda(_,MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_))
                                                     OptionalQuote (Lambda([ParamName sourceAlias],TupleSqlColumnsGet(multisource)))
                                                     OptionalQuote (Lambda([ParamName destAlias],TupleSqlColumnsGet(multidest)))
                                                     OptionalQuote (Lambda(projectionParams,_))]) ->
                                let outExp = processSelectManys projectionParams.[0].Name createRelated outExp

                                let destKeys = multidest |> List.map(fun (_,destKey,_) -> destKey)
                                let aliashandlesSource =
                                    multisource |> List.map(
                                        fun (sourceTi,sourceKey,_) ->
                                            let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                                            if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                            if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                            sourceAlias, sourceKey
                                        )
                                let sourceAlias = match aliashandlesSource with [] -> sourceAlias | (alias,_)::t -> alias
                                let sourceKeys = aliashandlesSource |> List.map snd

                                let data = { PrimaryKey = destKeys; PrimaryTable = Table.FromFullName destEntity; ForeignKey = sourceKeys;
                                                ForeignTable = {Schema="";Name="";Type=""};
                                                OuterJoin = false; RelDirection = RelationshipDirection.Parents }
                                SelectMany(sourceAlias,destAlias,LinkQuery(data),outExp)
                            | _ -> failwith ("Unknown: " + inExp.ToString())

                        let ex = processSelectManys projectionParams.[1].Name inner source.SqlExpression
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; ex; source.TupleIndex;|] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "Select"), [ SourceWithQueryData source; OptionalQuote (Lambda([ v1 ], _) as lambda) ]) as whole ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType((lambda :?> LambdaExpression).ReturnType )
                        if v1.Name.StartsWith "_arg" && v1.Type <> typeof<SqlEntity> && not(v1.Type.Name.StartsWith("IGrouping")) then
                            // this is the projection from a join - ignore
                            // causing the ignore here will give us wrong return tyoe to deal with in convertExpression lambda handling
                            ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; source.SqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                        else
                            ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; Projection(whole,source.SqlExpression); source.TupleIndex;|] :?> IQueryable<_>

                    | MethodCall(None,(MethodWithName("Union") | MethodWithName("Concat") as meth), [SourceWithQueryData source; SeqValuesQueryable values]) when (values :? IWithSqlService) -> 

                        let subquery = values :?> IWithSqlService
                        use con = subquery.Provider.CreateConnection(source.DataContext.ConnectionString)
                        let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression subquery.SqlExpression subquery.TupleIndex con subquery.Provider

                        let modified = 
                            parameters |> Seq.map(fun p ->
                                p.ParameterName <- p.ParameterName.Replace("@param", "@paramnested")
                                p
                            ) |> Seq.toArray
                        let subquery = 
                            let paramfixed = query.Replace("@param", "@paramnested")
                            match paramfixed.EndsWith(";") with
                            | false -> paramfixed
                            | true -> paramfixed.Substring(0, paramfixed.Length-1)

                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        let all = meth.Name = "Concat"
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; Union(all,subquery,source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | x -> failwith ("unrecognised method call " + x.ToString())

                member __.Execute(_: Expression) : obj =
                    failwith "Execute not implemented"

                member __.Execute<'T>(e: Expression) : 'T =
                    Common.QueryEvents.PublishExpression e
                    match e with
                    | MethodCall(_, (MethodWithName "First"), [Constant(query, _)]) ->
                        let svc = (query :?> IWithSqlService)
                        executeQuery svc.DataContext svc.Provider (Take(1,(svc.SqlExpression))) svc.TupleIndex
                        |> Seq.cast<'T>
                        |> Seq.head
                    | MethodCall(_, (MethodWithName "FirstOrDefault"), [Constant(query, _)]) ->
                        let svc = (query :?> IWithSqlService)
                        executeQuery svc.DataContext svc.Provider (Take(1, svc.SqlExpression)) svc.TupleIndex
                        |> Seq.cast<'T>
                        |> Seq.tryFind (fun _ -> true)
                        |> Option.fold (fun _ x -> x) Unchecked.defaultof<'T>
                    | MethodCall(_, (MethodWithName "Single"), [Constant(query, _)]) ->
                        match (query :?> seq<_>) |> Seq.toList with
                        | x::[] -> x
                        | [] -> raise <| InvalidOperationException("Encountered zero elements in the input sequence")
                        | _ -> raise <| InvalidOperationException("Encountered more than one element in the input sequence")
                    | MethodCall(_, (MethodWithName "SingleOrDefault"), [Constant(query, _)]) ->
                        match (query :?> seq<_>) |> Seq.toList with
                        | [] -> Unchecked.defaultof<'T>
                        | x::[] -> x
                        | _ -> raise <| InvalidOperationException("Encountered more than one element in the input sequence")
                    | MethodCall(None, (MethodWithName "Count"), [Constant(query, _)]) ->
                        let svc = (query :?> IWithSqlService)
                        let res = executeQueryScalar svc.DataContext svc.Provider (Count(svc.SqlExpression)) svc.TupleIndex 
                        (Utilities.convertTypes res typeof<'T>) :?> 'T
                    | MethodCall(None, (MethodWithName "Any" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        let limitedSource = 
                            {new IWithSqlService with 
                                member t.DataContext = source.DataContext
                                member t.SqlExpression = Take(1, source.SqlExpression) 
                                member t.Provider = source.Provider
                                member t.TupleIndex = source.TupleIndex }
                        let res = parseWhere meth limitedSource qual
                        res |> Seq.length > 0 |> box :?> 'T
                    | MethodCall(None, (MethodWithName "All" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        let negativeCheck = 
                            match qual with
                            | :? LambdaExpression as la -> Expression.Lambda(Expression.Not(la.Body), la.Parameters) :> Expression
                            | _ -> Expression.Not(qual) :> Expression

                        let limitedSource = 
                            {new IWithSqlService with 
                                member t.DataContext = source.DataContext
                                member t.SqlExpression = Take(1, source.SqlExpression) 
                                member t.Provider = source.Provider
                                member t.TupleIndex = source.TupleIndex }
                        
                        let res = parseWhere meth limitedSource negativeCheck
                        res |> Seq.length = 0 |> box :?> 'T
                    | MethodCall(None, (MethodWithName "First" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        let limitedSource = 
                            {new IWithSqlService with 
                                member t.DataContext = source.DataContext
                                member t.SqlExpression = Take(1, source.SqlExpression) 
                                member t.Provider = source.Provider
                                member t.TupleIndex = source.TupleIndex }
                        let res = parseWhere meth limitedSource qual
                        res |> Seq.head |> box :?> 'T
                    | MethodCall(None, (MethodWithName "Average" | MethodWithName "Sum" | MethodWithName "Max" | MethodWithName "Min" as meth), [SourceWithQueryData source; 
                             OptionalQuote (Lambda([ParamName param], OptionalConvertOrTypeAs(SqlColumnGet(entity,key,_)))) 
                             ]) ->
                        let alias =
                             match entity with
                             | "" when source.SqlExpression.HasAutoTupled() -> param
                             | "" -> ""
                             | _ -> resolveTuplePropertyName entity source.TupleIndex
                        let sqlExpression =
                               
                               match meth.Name, source.SqlExpression with
                               | "Sum", BaseTable("",entity)  -> AggregateOp(Sum(None),"",key,BaseTable(alias,entity))
                               | "Sum", _ ->  AggregateOp(Sum(None),alias,key,source.SqlExpression)
                               | "Max", BaseTable("",entity)  -> AggregateOp(Max(None),"",key,BaseTable(alias,entity))
                               | "Max", _ ->  AggregateOp(Max(None),alias,key,source.SqlExpression)
                               | "Min", BaseTable("",entity)  -> AggregateOp(Min(None),"",key,BaseTable(alias,entity))
                               | "Min", _ ->  AggregateOp(Min(None),alias,key,source.SqlExpression)
                               | "Average", BaseTable("",entity)  -> AggregateOp(Avg(None),"",key,BaseTable(alias,entity))
                               | "Average", _ ->  AggregateOp(Avg(None),alias,key,source.SqlExpression)
                               | _ -> failwithf "Unsupported aggregation `%s` in execution expression `%s`" meth.Name (e.ToString())
                        let res = executeQueryScalar source.DataContext source.Provider sqlExpression source.TupleIndex 
                        (Utilities.convertTypes res typeof<'T>) :?> 'T
                    | MethodCall(None, (MethodWithName "Contains"), [SourceWithQueryData source; 
                             OptionalQuote(OptionalFSharpOptionValue(ConstantOrNullableConstant(c))) 
                             ]) ->
                             
                        let sqlExpression =
                            match source.SqlExpression with 
                            | Projection(MethodCall(None, _, [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]),BaseTable(alias,entity2)) ->
                                Count(Take(1,(FilterClause(Condition.And([alias, key, ConditionOperator.Equal, c],None),source.SqlExpression))))
                            | Projection(MethodCall(None, _, [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]), current) ->
                                Count(Take(1,(FilterClause(Condition.And(["", key, ConditionOperator.Equal, c],None),current))))
                            | others ->
                                failwithf "Unsupported execution of contains expression `%s`" (e.ToString())

                        let res = executeQueryScalar source.DataContext source.Provider sqlExpression source.TupleIndex 
                        (Utilities.convertTypes res typeof<'T>) :?> 'T
                    | MethodCall(_, (MethodWithName "ElementAt"), [SourceWithQueryData source; Int position ]) ->
                        let skips = position - 1
                        executeQuery source.DataContext source.Provider (Take(1,(Skip(skips,source.SqlExpression)))) source.TupleIndex
                        |> Seq.cast<'T>
                        |> Seq.head
                    | e -> failwithf "Unsupported execution expression `%s`" (e.ToString())  }


