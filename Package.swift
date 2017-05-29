import PackageDescription

let package = Package(
  name: "ZeeQL3Apache",

  targets: [
    Target(name: "APRAdaptor"),
    Target(name: "ApacheZeeQLAdaptor",
           dependencies: [ .Target(name: "APRAdaptor" ) ])
  ],
  
  dependencies: [
    .Package(url: "https://github.com/modswift/CApache.git", 
             majorVersion: 1, minor: 0),
    .Package(url: "https://github.com/ZeeQL/ZeeQL3.git", 
             majorVersion: 0)
  ],
	
  exclude: [
    "ZeeQL3Apache.xcodeproj",
    "GNUmakefile",
    "LICENSE",
    "README.md",
    "xcconfig"
  ]
)
