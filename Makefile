JAVAC = javac
JAVA = java

MAIN_CLASS = Tema1
SOURCES = $(wildcard *.java)
BIN_DIR = bin

CLASSPATH = $(BIN_DIR):libs/*

APP_ARGS := $(filter-out run build clean all,$(MAKECMDGOALS))

.PHONY: all build run clean

all: build

build: $(BIN_DIR) $(BIN_DIR)/Article.class $(BIN_DIR)/DataLoader.class $(BIN_DIR)/Sorter.class $(BIN_DIR)/OutputWriter.class $(BIN_DIR)/Tema1.class

$(BIN_DIR)/%.class: %.java
	$(JAVAC) -d $(BIN_DIR) -cp $(CLASSPATH) $<

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(BIN_DIR)/%.class: %.java
	$(JAVAC) -d $(BIN_DIR) -cp $(CLASSPATH) $<

%::
	@true

run: build
	$(JAVA) -cp $(CLASSPATH) $(MAIN_CLASS) $(ARGS)

clean:
	-rm -rf $(BIN_DIR)
