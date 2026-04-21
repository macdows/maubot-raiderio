build:
	rm -f raiderio.mbp
	zip -r raiderio.mbp raiderio_bot maubot.yaml base-config.yaml -x '*/__pycache__/*'
