// swift-tools-version:5.0
import PackageDescription

let package = Package(
  name: "ZeeQL3Apache",

  products: [
    .library(name: "APRAdaptor",         targets: [ "APRAdaptor" ]),
    .library(name: "ApacheZeeQLAdaptor", targets: [ "ApacheZeeQLAdaptor" ])
  ],
  dependencies: [
    .package(url: "https://github.com/modswift/CApache.git",
             from: "2.0.2"),
    .package(url: "https://github.com/ZeeQL/ZeeQL3.git",
             from: "0.7.1")
  ],
  targets: [
    .target(name: "APRAdaptor",
            dependencies: [ "CApache", "ZeeQL" ]),
    .target(name: "ApacheZeeQLAdaptor",
            dependencies: [ "APRAdaptor" ])
  ]
)
