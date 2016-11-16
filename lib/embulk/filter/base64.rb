Embulk::JavaPlugin.register_filter(
  "base64", "org.embulk.filter.base64.Base64FilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
