import json

from copy import deepcopy
from typing import Dict, List, Optional

class Volume:
    """A volume that can be mounted on a container.

    :param host_path: The path from the host to be mounted.
    :type host_path: str
    :param container_path: Mount point inside the container. Defaults to None.
    :type container_path: str, optional
    :param mode: Permissions mode of the volume. Defaults to None.
    :type mode: str, optional
    """
    host_path: str

    container_path: Optional[str]
    mode: Optional[str]

    def __init__(self, *,
                 host_path: str,

                 container_path: Optional[str] = None,
                 mode: Optional[str] = None):
        self.host_path = host_path
        self.container_path = container_path
        self.mode = mode

    def __str__(self):
        return json.dumps(self.to_dict(), indent=4)

    def __repr__(self):
        inner = ', '.join(
            f'{key}={repr(value)}'
            for key, value in self.__dict__.items()
        )
        return f'Volume({inner})'

    def to_dict(self) -> dict:
        """Generate this volume's ``dict`` representation."""
        d = {'host_path': self.host_path}
        if self.container_path is not None:
            d['container_path'] = self.container_path
        if self.mode is not None:
            d['mode'] = self.mode
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Volume':
        """Create a volume from its ``dict`` representation."""
        d = deepcopy(d)
        return cls(**d)


class AbstractContainer:
    """Base class for containers to be used on Cook.

    Implementors must override the ``kind`` property, which will help indicate
    which subclass of the container to use when working with this class's
    ``dict`` representation.
    """
    volumes: Optional[List[Volume]]

    def __init__(self, *, volumes: Optional[List[Volume]] = None):
        self.volumes = volumes

    def __str__(self):
        return json.dumps(self.to_dict(), indent=4)

    @property
    def kind(self) -> str:
        raise NotImplementedError

    def to_dict(self) -> dict:
        d = {'type': self.kind}
        if self.volumes is not None:
            d['volumes'] = list(map(Volume.to_dict, self.volumes))
        return d


class DockerPortMapping:
    """A Docker port mapping.

    :param host_port: Port to open on the host machine.
    :type host_port: int
    :param container_port: Port which will be open inside the container.
    :type container_port: int
    :param protocol: Protocol of the port. Defaults to None.
    :type protocol: str, optional
    """
    host_port: int
    container_port: int

    protocol: Optional[str]

    def __init__(self, *,
                 host_port: int,
                 container_port: int,
                 protocol: Optional[str] = None):
        self.host_port = host_port
        self.container_port = container_port
        self.protocol = protocol

    def __str__(self):
        return json.dumps(self.to_dict(), indent=4)

    def __repr__(self):
        inner = ', '.join(
            f'{key}={repr(value)}'
            for key, value in self.__dict__.items()
        )
        return f'DockerPortMapping({inner})'

    def to_dict(self) -> dict:
        d = {
            'host_port': self.host_port,
            'container_port': self.container_port
        }
        if self.protocol is not None:
            d['protocol'] = self.protocol
        return d


class DockerContainer(AbstractContainer):
    """A Docker container.

    :param image: Name of the image to use.
    :type image: str
    :param network: Network the container should be in. Defaults to None.
    :type network: str, optional
    :param force_pull_image: If true, then the image will always be pulled.
        Defaults to None.
    :type force_pull_image: bool, optional
    :param parameters: Container parameters. Defaults to None.
    :type parameters: List[Dict[str, str]], optional
    :param port_mapping: List of port mappings to apply to the container.
        Defaults to None.
    :type port_mapping: List[DockerPortMapping], optional
    """
    image: str

    network: Optional[str]
    force_pull_image: Optional[bool]
    parameters: Optional[List[Dict[str, str]]]
    port_mapping: Optional[List[DockerPortMapping]]

    def __init__(self, image: str, *,

                 network: Optional[str] = None,
                 force_pull_image: Optional[bool] = None,
                 parameters: Optional[List[Dict[str, str]]] = None,
                 port_mapping: Optional[List[DockerPortMapping]] = None,

                 volumes: Optional[List[Volume]] = None):
        self.image = image
        self.network = network
        self.force_pull_image = force_pull_image
        self.parameters = parameters
        self.port_mapping = port_mapping
        super().__init__(volumes=volumes)

    def __repr__(self):
        inner = ', '.join(
            f'{key}={repr(value)}'
            for key, value in self.__dict__.items()
        )
        return f'DockerContainer({inner})'

    @property
    def kind(self) -> str:
        """Get the kind of this container, which is ``'docker'``."""
        return 'docker'

    def to_dict(self) -> dict:
        """Get the ``dict`` representation of this container."""
        d = super().to_dict()
        d['docker'] = {'image': self.image}
        ddocker = d['docker']

        if self.network is not None:
            ddocker['network'] = self.network
        if self.force_pull_image is not None:
            ddocker['force_pull_image'] = self.force_pull_image
        if self.parameters is not None:
            ddocker['parameters'] = self.parameters
        if self.port_mapping is not None:
            ddocker['port_mapping'] = list(map(DockerPortMapping.to_dict,
                                               self.port_mapping))
        return d
