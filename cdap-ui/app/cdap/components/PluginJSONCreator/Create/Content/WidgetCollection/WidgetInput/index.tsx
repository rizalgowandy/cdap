/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import If from 'components/If';
import { WIDGET_TYPES, WIDGET_TYPE_TO_ATTRIBUTES } from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    widgetInput: {
      '& > *': {
        marginTop: '10px',
        marginBottom: '10px',
      },
    },
  };
};

const WidgetInputView: React.FC<WithStyles<typeof styles>> = ({
  classes,
  widgetID,
  widgetToInfo,
  setWidgetToInfo,
  widgetToAttributes,
  setWidgetToAttributes,
}) => {
  function onNameChange(obj, id) {
    return (name) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, name } }));
    };
  }

  function onLabelChange(obj, id) {
    return (label) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, label } }));
    };
  }

  function onWidgetTypeChange(obj, id) {
    return (widgetType) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, widgetType } }));

      setWidgetToAttributes({
        ...widgetToAttributes,
        [id]: Object.keys(WIDGET_TYPE_TO_ATTRIBUTES[widgetType]).reduce((acc, curr) => {
          acc[curr] = '';
          return acc;
        }, {}),
      });
    };
  }

  function onWidgetCategoryChange(obj, id) {
    return (widgetCategory) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, widgetCategory } }));
    };
  }

  const widgetInfo = widgetToInfo[widgetID];

  return React.useMemo(
    () => (
      <If condition={widgetInfo}>
        <div className={classes.widgetInput}>
          <PluginInput
            widgetType={'textbox'}
            value={widgetInfo.name}
            setValue={onNameChange(widgetInfo, widgetID)}
            label={'Name'}
            placeholder={'Name a Widget'}
            required={false}
          />
          <PluginInput
            widgetType={'textbox'}
            value={widgetInfo.label}
            setValue={onLabelChange(widgetInfo, widgetID)}
            label={'Label'}
            placeholder={'Label a Widget'}
            required={false}
          />
          <PluginInput
            widgetType={'textbox'}
            value={widgetInfo.widgetCategory}
            setValue={onWidgetCategoryChange(widgetInfo, widgetID)}
            label={'Category'}
            placeholder={'Categorize a Widget'}
            required={false}
          />
          <PluginInput
            widgetType={'select'}
            value={widgetInfo.widgetType}
            setValue={onWidgetTypeChange(widgetInfo, widgetID)}
            label={'Widget Type'}
            options={WIDGET_TYPES}
            required={true}
          />
        </div>
      </If>
    ),
    [widgetToInfo[widgetID], widgetToAttributes[widgetID]]
  );
};

const WidgetInput = withStyles(styles)(WidgetInputView);
export default WidgetInput;
