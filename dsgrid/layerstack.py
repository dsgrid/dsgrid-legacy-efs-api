
from layerstack.layer import ModelLayerBase

from dsgrid import DSGridValueError
from dsgrid.dataformat.datafile import Datafile

class DSGridDatafileLayer(ModelLayerBase):

    @classmethod
    def _check_model_type(cls, model):
        """
        Checks model to ensure that it is a dsgrid.dataformat.datafile.Datafile.
        Raises a DSGridValueError if model is not of that type.

        Parameters
        ----------
        model : dsgrid.dataformat.datafile.Datafile
            Datafile to be operated on

        Returns
        -------
        None
        """
        if not isinstance(model,Datafile):
            raise DSGridValueError('Expecting a {}, but got a {}'.format(Datafile,type(model)))

    @classmethod
    def _load_model(cls, model_path):
        """
        Load dsgrid.dataformat.datafile.Datafile from model_path.

        Parameters
        ----------
        model_path : 'str'
            Path to dsgrid.dataformat.datafile.Datafile

        Returns
        -------
        dsgrid.dataformat.datafile.Datafile
            Datafile to be operated on
        """
        return Datafile.load(model_path)

    @classmethod
    def _save_model(cls, model, model_path):
        """
        Save model from cli. Derived classes must implement this method if they 
        support saving the model out to disk.

        Parameters
        ----------
        model 
            model to be saved to disk
        model_path : 'str'
            Path to save model to
        """
        cls._check_model_type(model)
        model.save(model_path)

