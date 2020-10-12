Embulk::JavaPlugin.register_input(
  "sparql", "org.embulk.input.sparql.SparqlInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
