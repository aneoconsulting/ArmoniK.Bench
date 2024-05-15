from typing import Any


def _get_control_plane_url_without_prefix(terraform_outputs: dict[str, Any]) -> str:
    """
    Get the control plane URL without the 'http://' or 'https://' prefix.

    Args:
        terraform_outputs (dict[str, Any]): Terraform outputs dictionary.

    Returns:
        str: Control plane URL without prefix.
    """
    return (
        terraform_outputs["armonik"]["value"]["control_plane_url"]
        .removeprefix("https://")
        .removeprefix("http://")
    )


def get_control_plane_host_from_tf_outputs(terraform_outputs: dict[str, Any]) -> str:
    """
    Get ArmoniK control plane host from Terraform outputs.

    Args:
        terraform_outputs (dict[str, Any]): Terraform outputs dictionary.

    Returns:
        str: ArmoniK control plane host.
    """
    return _get_control_plane_url_without_prefix(terraform_outputs).split(":")[0]


def get_control_plane_port_from_tf_outputs(terraform_outputs: dict[str, Any]) -> str:
    """
    Get ArmoniK control plane port from Terraform outputs.

    Args:
        terraform_outputs (dict[str, Any]): Terraform outputs dictionary.

    Returns:
        str: ArmoniK control plane port.
    """
    return _get_control_plane_url_without_prefix(terraform_outputs).split(":")[1]
