all :

clean :

xcodebuild :
	xcodebuild -workspace ZeeQL3Apache.xcworkspace -scheme APRAdaptor

spm-build:
	swift build

