// --------------------------------------------------------------------------------------
// FAKE build script
// --------------------------------------------------------------------------------------

#r "paket: groupref Build //"
#load "./.fake/build.fsx/intellisense.fsx"

#if !FAKE
  #r "netstandard"
#endif

open Fake.Core
open Fake.Core.TargetOperators
open Fake.Tools
open Fake.DotNet
open Fake.IO
open Fake.IO.Globbing.Operators
open System

// --------------------------------------------------------------------------------------
// START TODO: Provide project-specific details below
// --------------------------------------------------------------------------------------

// Information about the project are used
//  - for version and project name in generated AssemblyInfo file
//  - by the generated NuGet package
//  - to run tests and to publish documentation on GitHub gh-pages
//  - for documentation, you also need to edit info in "docs/tools/generate.fsx"

// The name of the project
// (used by attributes in AssemblyInfo, name of a NuGet package and directory in 'src')

let project = "SQLProvider"

// Short summary of the project
// (used as description in AssemblyInfo and as a short summary for NuGet package)
let summary = "Type providers for SQL database access."

// Longer description of the project
// (used as a description for NuGet package; line breaks are automatically cleaned up)
let description = "Type providers for SQL database access."

// List of author names (for NuGet package)
let authors = [ "Ross McKinlay, Colin Bull, Tuomas Hietanen" ]

// Tags for your project (for NuGet package)
let tags = "F#, fsharp, typeprovider, sql, sqlserver"

// Pattern specifying assemblies to be tested using NUnit
let testAssemblies = "tests/**/bin/Release/*Tests*.dll"

// Git configuration (used for publishing documentation in gh-pages branch)
// The profile where the project is posted
let gitOwner = "fsprojects"
let gitHome = "https://github.com/" + gitOwner

// The name of the project on GitHub
let gitName = "SQLProvider"

// The url for the raw files hosted
let gitRaw = Environment.environVarOrDefault "gitRaw" "https://raw.github.com/fsprojects"

// --------------------------------------------------------------------------------------
// END TODO: The rest of the file includes standard build steps
// --------------------------------------------------------------------------------------
// Read additional information from the release notes document
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = ReleaseNotes.load "RELEASE_NOTES.md"

// Generate assembly info files with the right version & up-to-date information
Target.create "AssemblyInfo" (fun _ ->
  for assembly in (!! "src/*/*.fsproj") do
    let assemblyName = Path.GetFileNameWithoutExtension assembly  
    let fileName = "src/" + assemblyName + "/AssemblyInfo.fs"
    let assemblySummary = 
      match assemblyName with
      | "SQLProvider.DesignTime" -> summary + " Design-time component."
      | "SQLProvider.Runtime" -> summary + " Runtime component."
      | providerName -> summary + " Provider for " + (providerName.Substring("SQLProvider.".Length)) + "."

    AssemblyInfoFile.create 
        fileName
        [ AssemblyInfo.Title assemblyName
          AssemblyInfo.Product assemblyName
          AssemblyInfo.Description assemblySummary
          AssemblyInfo.Version release.AssemblyVersion
          AssemblyInfo.FileVersion release.AssemblyVersion ] 
        None
)

// --------------------------------------------------------------------------------------
// Clean build results & restore NuGet packages


Target.create "Clean" (fun _ ->
    ["bin"; "temp"] |> List.iter Directory.delete
)

Target.create "CleanDocs" (fun _ ->
    ["docs/output"] |> List.iter Directory.delete
)

// --------------------------------------------------------------------------------------
// Build library & test project

Target.create "Build" (fun _ ->
  
  
    // Build .NET Core solution
    DotNet.build(fun p -> 
        { p with 
            Configuration = DotNet.BuildConfiguration.Release})

        "src/SQLProvider/SQLProvider.fsproj"

    // Build .NET Framework solution
    !!"SQLProvider.sln" ++ "SQLProvider.Tests.sln"
    |> MSBuild.runReleaseExt id "" [ "DefineConstants", BuildServer.buildServer.ToString().ToUpper()] "Rebuild"
    |> ignore
)

// --------------------------------------------------------------------------------------
// Set up a PostgreSQL database in the CI pipeline to run tests

Target.create "SetupPostgreSQL" (fun _ ->
      let connBuilder = Npgsql.NpgsqlConnectionStringBuilder()
  
      connBuilder.Host <- "localhost"
      connBuilder.Port <- 5432
      connBuilder.Database <- "postgres"
      connBuilder.Username <- "postgres"
      connBuilder.Password <- 
        match BuildServer.buildServer with
        | Fake.Core.BuildServer.Travis -> ""
        | Fake.Core.BuildServer.AppVeyor -> "Password12!"
        | _ -> "postgres"      
  
      let runCmd query = 
        // We wait up to 30 seconds for PostgreSQL to be initialized
        let rec runCmd' attempt = 
          try
            use conn = new Npgsql.NpgsqlConnection(connBuilder.ConnectionString)
            conn.Open()
            use cmd = new Npgsql.NpgsqlCommand(query, conn)
            cmd.ExecuteNonQuery() |> ignore 
          with e -> 
            printfn "Connection attempt %i: %A" attempt e
            Threading.Thread.Sleep 1000
            if attempt < 30 then runCmd' (attempt + 1)
  
        runCmd' 0
                
      let testDbName = "sqlprovider"
      printfn "Creating test database %s on connection %s" testDbName connBuilder.ConnectionString
      runCmd (sprintf "CREATE DATABASE %s" testDbName)
      connBuilder.Database <- testDbName
  
      (!! "src/DatabaseScripts/PostgreSQL/*.sql")
      |> Seq.map (fun file -> printfn "Running script %s on connection %s" file connBuilder.ConnectionString; file)
      |> Seq.map IO.File.ReadAllText      
      |> Seq.iter runCmd
)

// --------------------------------------------------------------------------------------
// Set up a MS SQL Server database to run tests

let setupMssql url saPassword = 
    let connBuilder = Data.SqlClient.SqlConnectionStringBuilder()    
    connBuilder.InitialCatalog <- "master"
    connBuilder.UserID <- "sa"
    connBuilder.DataSource <- url
    connBuilder.Password <- saPassword   
          
    let runCmd query = 
      // We wait up to 30 seconds for MSSQL to be initialized
      let rec runCmd' attempt = 
        try
          use conn = new Data.SqlClient.SqlConnection(connBuilder.ConnectionString)
          conn.Open()
          use cmd = new Data.SqlClient.SqlCommand(query, conn)
          cmd.ExecuteNonQuery() |> ignore 
        with e -> 
          printfn "Connection attempt %i: %A" attempt e
          Threading.Thread.Sleep 1000
          if attempt < 30 then runCmd' (attempt + 1)

      runCmd' 0

    let runScript fileLines =            
            
      // We look for the 'GO' lines that complete the individual SQL commands
      let rec cmdGen cache (lines : string list) =
        seq {
          match cache, lines with
          | [], [] -> ()
          | cmds, [] -> yield cmds
          | cmds, l :: ls when l.Trim().ToUpper() = "GO" -> yield cmds; yield! cmdGen [] ls
          | cmds, l :: ls -> yield! cmdGen (l :: cmds) ls
        }      

      for cmd in cmdGen [] (fileLines |> Seq.toList) do
        let query = cmd |> List.rev |> String.concat "\r\n"
        runCmd query

    let testDbName = "sqlprovider"
    printfn "Creating test database %s on connection %s" testDbName connBuilder.ConnectionString
    runCmd (sprintf "CREATE DATABASE %s" testDbName)
    connBuilder.InitialCatalog <- testDbName

    (!! "src/DatabaseScripts/MSSQLServer/*.sql")
    |> Seq.map (fun file -> printfn "Running script %s on connection %s" file connBuilder.ConnectionString; file)
    |> Seq.map IO.File.ReadAllLines
    |> Seq.iter runScript
    
Target.create "SetupMSSQL2008R2" (fun _ ->
    setupMssql "(local)\SQL2008R2SP2" "Password12!"
)

Target.create "SetupMSSQL2017" (fun _ ->
    setupMssql "(local)\SQL2017" "Password12!"
)


// --------------------------------------------------------------------------------------
// Run the unit tests using test runner

Target.create "RunTests" (fun _ ->
    !! testAssemblies 
    |> Testing.NUnit.Sequential.run (fun p ->
        { p with
            DisableShadowCopy = true
            TimeOut = TimeSpan.FromMinutes 20.
            OutputFile = "TestResults.xml" })
)

// --------------------------------------------------------------------------------------
// Build a NuGet package

Target.create "PackNuGet" (fun _ -> 
    Paket.pack(fun p -> 
        { p with 
            Version = release.NugetVersion
            ReleaseNotes = String.Join(Environment.NewLine, release.Notes)
            Symbols = true
            OutputPath = "bin" })
    Fake.Tools.Git.Branches.tag "" release.NugetVersion
) 

// --------------------------------------------------------------------------------------
// Generate the documentation

Target.create "GenerateReferenceDocs" (fun _ ->
    FSFormatting.createDocsForDlls id [ 
      "FSharp.Data.SqlProvider.dll" 
      "FSharp.Data.SqlProvider.Common.dll" 
      "FSharp.Data.SqlProvider.PostgreSQL.dll" 
    ]
)

Target.create "GenerateHelp" (fun _ ->
    File.delete "docs/content/release-notes.md"
    Shell.copyFile "docs/content/" "RELEASE_NOTES.md"
    Shell.rename "docs/content/release-notes.md" "docs/content/RELEASE_NOTES.md"

    File.delete "docs/content/license.md"
    Shell.copyFile "docs/content/" "LICENSE.txt"
    Shell.rename "docs/content/license.md" "docs/content/LICENSE.txt"

    Shell.copyFile "bin/net451" "packages/FSharp.Core/lib/net40/FSharp.Core.sigdata"
    Shell.copyFile "bin/net451" "packages/FSharp.Core/lib/net40/FSharp.Core.optdata"

    FSFormatting.createDocs (fun p ->
      
      // Web site location for the generated documentation
      let website = "/SQLProvider"

      let githubLink = "http://github.com/fsprojects/SQLProvider"
      { p with
          Source = "docs/"
          OutputDirectory = "docs/content"

      // Specify more information about your project
          ProjectParameters =
            [ "project-name", "SQLProvider"
              "project-author", "Ross McKinlay, Colin Bull, Tuomas Hietanen"
              "project-summary", "Type providers for SQL server access."
              "project-github", "http://github.com/fsprojects/SQLProvider"
              "project-nuget", "http://nuget.org/packages/SQLProvider" 
            ]
      }
    )
)


// --------------------------------------------------------------------------------------
// Release Scripts

Target.create "ReleaseDocs" (fun _ ->
    let tempDocsDir = "temp/gh-pages"
    Shell.cleanDir tempDocsDir
    Git.Repository.cloneSingleBranch "" (gitHome + "/" + gitName + ".git") "gh-pages" tempDocsDir

    Shell.cleanDir tempDocsDir
    Shell.copyRecursive "docs/output" tempDocsDir true |> Trace.tracefn "%A"
    Git.Staging.stageAll tempDocsDir
    Git.Commit.exec tempDocsDir (sprintf "Update generated documentation for version %s" release.NugetVersion)
    Git.Branches.push tempDocsDir
)

Target.create "Release" ignore

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target.create "All" ignore


Target.create "BuildDocs" ignore

"Clean"
  ==> "AssemblyInfo"  
  // In CI mode, we setup a Postgres database before building
  =?> ("SetupPostgreSQL", not BuildServer.isLocalBuild)
  // On AppVeyor, we also add a SQL Server 2008R2 one and a SQL Server 2017 for compatibility
  =?> ("SetupMSSQL2008R2", BuildServer.buildServer = Fake.Core.BuildServer.AppVeyor)
  =?> ("SetupMSSQL2017", BuildServer.buildServer = Fake.Core.BuildServer.AppVeyor)
  ==> "Build"
  ==> "RunTests"
  ==> "CleanDocs"
  // Travis doesn't support mono+dotnet:
  //==> "GenerateReferenceDocs"
  //==> "GenerateHelp"
  ==> "All"

//"GenerateReferenceDocs"
//  ==> "GenerateHelpDebug"

"All"
  ==> "BuildDocs"

"All" 
  ==> "ReleaseDocs"
  ==> "Release"

"All" 
  ==> "PackNuGet"

Target.runOrDefault "All"
