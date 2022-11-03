import * as React from 'react';
import CircularProgress from '@mui/material/CircularProgress';
import Button from '@mui/material/Button';
import { useGetCollectionAssociationsQuery, useDeleteSourceCollectionAssociationMutation } from '../../app/services/sourcesCollectionsApi';
import SourceItem from './SourceItem';

export default function SourceList(props) {
  const { collectionId, edit } = props;
  const {
    data,
    isLoading,
  } = useGetCollectionAssociationsQuery(collectionId);

  const [deleteSourceCollectionAssociation] = useDeleteSourceCollectionAssociationMutation();

  // if loading
  if (isLoading) {
    return (
      <div>
        {' '}
        <CircularProgress size="75px" />
        {' '}
      </div>
    );
  }
  // if edit
  if (edit) {
    return (
      <div className="collectionAssociations">
        {/* Header */}
        <h2>
          This Collection has
          {data.sources.length}
          {' '}
          Sources
        </h2>
        {data.sources.map((source) => (
          <div className="collectionItem" key={`edit-${source.id}`}>

            {/* Source */}
            <SourceItem source={source} />

            {/* Remove */}
            <Button onClick={() => {
              deleteSourceCollectionAssociation({
                source_id: source.id,
                collection_id: collectionId,
              });
            }}
            >
              Remove
            </Button>
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="collectionAssociations">

      {/* Header */}
      <h2>
        Associated with
        {data.sources.length}
        {' '}
        Sources
      </h2>
      {data.sources.map((source) => (
        <div className="collectionItem" key={`${source.id}`}>

          {/* Source */}
          <SourceItem key={`source-${source.id}`} source={source} />
        </div>
      ))}
    </div>
  );
}