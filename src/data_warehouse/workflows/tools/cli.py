"""
Command-line interface for the data warehouse workflow system.

This module provides a CLI tool for managing workflows, including listing,
executing, validating, and creating workflows from templates.
"""

import logging
import sys

import click

from data_warehouse.workflows.core.workflow_manager import WorkflowManager
from data_warehouse.workflows.tools.templates import TemplateGenerator, TemplateParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output (debug logging)")
def cli(verbose: bool) -> None:
    """
    Data Warehouse Workflow Management CLI.

    This tool provides commands for managing data warehouse workflows,
    including listing, executing, validating, and creating workflows.
    """
    if verbose:
        logging.getLogger("workflows").setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")


@cli.command("list")
@click.option(
    "--component-type",
    "-t",
    type=click.Choice(["extractors", "transformers", "loaders", "pipelines", "all"]),
    default="all",
    help="Type of components to list",
)
def list_components(component_type: str) -> None:
    """
    List available workflow components.

    This command displays all registered workflow components, optionally
    filtered by type (extractors, transformers, loaders, pipelines).
    """
    workflow_manager = WorkflowManager()
    workflow_manager.discover_components()

    if component_type in ["extractors", "all"]:
        extractors = workflow_manager.get_all_extractors()
        if extractors:
            click.echo(click.style("Extractors:", bold=True))
            for name, extractor in extractors.items():
                click.echo(f"  - {name} ({extractor.__class__.__name__})")
        else:
            click.echo("No extractors registered")

    if component_type in ["transformers", "all"]:
        transformers = workflow_manager.get_all_transformers()
        if transformers:
            click.echo(click.style("Transformers:", bold=True))
            for name, transformer in transformers.items():
                click.echo(f"  - {name} ({transformer.__class__.__name__})")
        else:
            click.echo("No transformers registered")

    if component_type in ["loaders", "all"]:
        loaders = workflow_manager.get_all_loaders()
        if loaders:
            click.echo(click.style("Loaders:", bold=True))
            for name, loader in loaders.items():
                click.echo(f"  - {name} ({loader.__class__.__name__})")
        else:
            click.echo("No loaders registered")

    if component_type in ["pipelines", "all"]:
        pipelines = workflow_manager.get_all_pipelines()
        if pipelines:
            click.echo(click.style("Pipelines:", bold=True))
            for name, pipeline in pipelines.items():
                click.echo(f"  - {name}")
                click.echo(f"      Extractor: {pipeline.extractor.name}")
                click.echo(f"      Transformers: {', '.join(t.name for t in pipeline.transformers)}")
                if pipeline.loader:
                    click.echo(f"      Loader: {pipeline.loader.name}")
        else:
            click.echo("No pipelines registered")


@cli.command("execute")
@click.argument("pipeline_name")
@click.option(
    "--template",
    "-t",
    required=True,  # Make template mandatory for execution now
    help="Path to the YAML/JSON template file defining the pipeline and its components.",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output during execution")
def execute_pipeline(pipeline_name: str, template: str, verbose: bool) -> None:
    """
    Execute a workflow pipeline defined in a template file.

    This command loads the specified template, creates the pipeline instance,
    and then runs it, processing data through its extractor, transformers,
    and loader components.
    """
    if verbose:
        logging.getLogger("workflows").setLevel(logging.DEBUG)
        # Assuming root logger or specific loggers might need adjustment too
        logging.getLogger().setLevel(logging.DEBUG)

    workflow_manager = WorkflowManager()
    # Discovery happens implicitly within create_pipeline_from_template or should be called
    # Let's ensure components are discovered before creating from template
    workflow_manager.discover_components()

    try:
        # Create/Register the pipeline from the template first
        click.echo(f"Loading pipeline '{pipeline_name}' from template: {template}")
        # Ensure the create method doesn't error if pipeline *instance* already exists
        # (though in this CLI flow, it likely won't)
        pipeline_instance = workflow_manager.create_pipeline_from_template(template)

        # Check if the loaded pipeline name matches the expected one
        if pipeline_instance.name != pipeline_name:
            logger.warning(
                f"Executing pipeline '{pipeline_instance.name}' loaded from template, "
                f"which differs from the provided argument '{pipeline_name}'."
            )
            # Decide whether to proceed with loaded name or error out.
            # Proceeding for now.
            pipeline_to_execute = pipeline_instance.name
        else:
            pipeline_to_execute = pipeline_name

        # Execute the pipeline by the name confirmed/loaded from the template
        click.echo(f"Executing pipeline: {pipeline_to_execute}")
        result = workflow_manager.execute_pipeline(pipeline_to_execute)
        click.echo(click.style("Pipeline executed successfully", fg="green"))
        # Optionally display result or summary here
        # click.echo(f"Result: {result}")
    except Exception as e:
        click.echo(click.style(f"Error executing pipeline: {str(e)}", fg="red"))
        logger.exception("Pipeline execution error")
        sys.exit(1)


@cli.command("validate")
@click.option("--pipeline", "-p", help="Validate a specific pipeline (default: validate all)")
@click.option("--template", "-t", help="Validate a pipeline template file")
def validate(pipeline: str | None, template: str | None) -> None:
    """
    Validate workflow components or templates.

    This command validates the configuration and compatibility of workflow
    components, either as registered pipelines or from template files.
    """
    workflow_manager = WorkflowManager()
    workflow_manager.discover_components()

    if template:
        try:
            click.echo(f"Validating template file: {template}")
            template_parser = TemplateParser()
            template_parser.parse_and_validate(template)
            click.echo(click.style("Template is valid", fg="green"))
        except Exception as e:
            click.echo(click.style(f"Template validation error: {str(e)}", fg="red"))
            sys.exit(1)
    elif pipeline:
        try:
            click.echo(f"Validating pipeline: {pipeline}")
            pipeline_obj = workflow_manager.get_pipeline(pipeline)
            workflow_manager.validator.validate_pipeline(pipeline_obj)
            click.echo(click.style("Pipeline is valid", fg="green"))
        except Exception as e:
            click.echo(click.style(f"Pipeline validation error: {str(e)}", fg="red"))
            sys.exit(1)
    else:
        try:
            click.echo("Validating all workflow components")
            workflow_manager.validate_workflow()
            click.echo(click.style("All components are valid", fg="green"))
        except Exception as e:
            click.echo(click.style(f"Validation error: {str(e)}", fg="red"))
            sys.exit(1)


@cli.command("create-template")
@click.option("--pipeline", "-p", help="Create a template from an existing pipeline")
@click.option("--output", "-o", required=True, help="Output file path for the template")
@click.option("--format", "-f", type=click.Choice(["yaml", "json"]), default="yaml", help="Template format")
@click.option("--example", is_flag=True, help="Create an example template instead of from a pipeline")
def create_template(pipeline: str | None, output: str, format: str, example: bool) -> None:
    """
    Create a workflow template.

    This command generates a template file (YAML or JSON) either from an existing
    pipeline or as an example template with sample components.
    """
    template_generator = TemplateGenerator()

    if example:
        try:
            click.echo(f"Creating example template: {output}")
            template_generator.create_example_template(output, format)
            click.echo(click.style(f"Example template created at: {output}", fg="green"))
        except Exception as e:
            click.echo(click.style(f"Error creating example template: {str(e)}", fg="red"))
            sys.exit(1)
    elif pipeline:
        try:
            workflow_manager = WorkflowManager()
            workflow_manager.discover_components()

            click.echo(f"Creating template from pipeline {pipeline}: {output}")
            workflow_manager.create_template_from_pipeline(pipeline, output, format)
            click.echo(click.style(f"Template created at: {output}", fg="green"))
        except Exception as e:
            click.echo(click.style(f"Error creating template: {str(e)}", fg="red"))
            sys.exit(1)
    else:
        click.echo(click.style("Error: Either --pipeline or --example is required", fg="red"))
        sys.exit(1)


@cli.command("create-pipeline")
@click.argument("template_file")
def create_pipeline(template_file: str) -> None:
    """
    Create and register components and pipeline(s) from a template file.

    Loads a template, instantiates and registers the defined components
    (extractors, transformers, loaders), and then creates and registers
    the pipeline(s) defined within it.
    """
    workflow_manager = WorkflowManager()
    # Discover components from standard locations first
    workflow_manager.discover_components()

    try:
        click.echo(f"Creating pipeline from template: {template_file}")
        pipeline = workflow_manager.create_pipeline_from_template(template_file)
        click.echo(click.style(f"Pipeline '{pipeline.name}' created successfully", fg="green"))
    except Exception as e:
        click.echo(click.style(f"Error creating pipeline: {str(e)}", fg="red"))
        logger.exception("Pipeline creation error")  # Log full traceback on error
        sys.exit(1)


@cli.command("watch")
@click.option("--directory", "-d", multiple=True, help="Directory to watch (default: standard workflow directories)")
def watch(directory: list[str]) -> None:
    """
    Watch workflow directories for changes.

    This command monitors the workflow directories for changes and
    automatically reloads components when files are created or modified.
    """
    try:
        workflow_manager = WorkflowManager()

        # Define the reload callback
        def reload_callback(file_path: str) -> None:
            click.echo(f"Reloading components due to change in: {file_path}")
            workflow_manager.reload_components()
            click.echo("Components reloaded")

        # Configure directories to watch
        directories = list(directory) if directory else None

        # Create and start the watcher
        click.echo("Starting workflow watcher...")
        # Dynamically import WorkflowWatcher to avoid circular dependency
        # if watchdog is not installed
        from data_warehouse.workflows.tools.watcher import WorkflowWatcher

        with WorkflowWatcher(directories=directories, reload_callback=reload_callback):
            click.echo("Watching workflow directories for changes (press Ctrl+C to stop)")
            try:
                # Keep the watcher running until interrupted
                while True:
                    import time

                    time.sleep(1)
            except KeyboardInterrupt:
                click.echo("Stopping workflow watcher...")

    except ImportError:
        click.echo(
            click.style("Error: watchdog library not installed. Install it with: pip install watchdog", fg="red")
        )
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"Error starting watcher: {str(e)}", fg="red"))
        sys.exit(1)


if __name__ == "__main__":
    cli()
