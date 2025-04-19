"""
Command-line interface for the data warehouse workflow system.

This module provides a CLI tool for managing workflows, including listing,
executing, validating, and creating workflows from templates.
"""

import logging
import sys

import click

from workflows.templates import TemplateGenerator, TemplateParser
from workflows.workflow_manager import WorkflowManager

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
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output during execution")
def execute_pipeline(pipeline_name: str, verbose: bool) -> None:
    """
    Execute a workflow pipeline.

    This command runs a specified pipeline, processing data through its
    extractor, transformers, and loader components.
    """
    if verbose:
        logging.getLogger("workflows").setLevel(logging.DEBUG)

    workflow_manager = WorkflowManager()
    workflow_manager.discover_components()

    try:
        click.echo(f"Executing pipeline: {pipeline_name}")
        result = workflow_manager.execute_pipeline(pipeline_name)
        click.echo(click.style("Pipeline executed successfully", fg="green"))
        return result
    except Exception as e:
        click.echo(click.style(f"Error executing pipeline: {str(e)}", fg="red"))
        if verbose:
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
    Create a pipeline from a template file.

    This command loads a template file and creates a new pipeline,
    registering all necessary components.
    """
    try:
        workflow_manager = WorkflowManager()
        workflow_manager.discover_components()

        click.echo(f"Creating pipeline from template: {template_file}")
        pipeline = workflow_manager.create_pipeline_from_template(template_file)
        workflow_manager.register_pipeline(pipeline)
        click.echo(click.style(f"Pipeline '{pipeline.name}' created successfully", fg="green"))
    except Exception as e:
        click.echo(click.style(f"Error creating pipeline: {str(e)}", fg="red"))
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
        from workflows.watcher import WorkflowWatcher

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
